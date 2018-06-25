using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using Dapper;
using Hangfire.Logging;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Entities;
using Npgsql;

namespace Hangfire.PostgreSql
{
    internal sealed class DatabaseInitializer
    {
        private static readonly ILog Log = LogProvider.GetLogger(typeof(PostgreSqlStorage));

        private readonly IConnectionProvider _connectionProvider;
        private readonly string _schemaName;

        public DatabaseInitializer(IConnectionProvider connectionProvider, string schemaName)
        {
            _connectionProvider = connectionProvider;
            _schemaName = schemaName;
        }

        public void Initialize()
        {
            Log.Info("Start installing Hangfire SQL objects...");

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var connection = connectionHolder.Connection;
                var locked = LockDatabase(connection);
                if (!locked) return;

                TryCreateSchema(connection);
                var installedVersion = GetInstalledVersion(connection);
                var availableMigrations = GetMigrations().Where(x => x.Version > installedVersion).ToArray();
                if (availableMigrations.Length == 0) return;

                using (var transaction = connection.BeginTransaction(IsolationLevel.Serializable))
                {
                    var lastMigration = default(MigrationInfo);
                    foreach (var migration in availableMigrations)
                    {
                        connection.Execute(migration.Script, transaction: transaction);
                        lastMigration = migration;
                    }

                    connection.Execute(
                        @"UPDATE schema SET version = @version WHERE version = @installedVersion",
                        new { lastMigration.Version, installedVersion },
                        transaction);

                    transaction.Commit();
                }

                UnlockDatabase(connection);
            }

            Log.Info("Hangfire SQL objects installed.");
        }

        private bool LockDatabase(NpgsqlConnection connection) => connection.Query<bool>(@"SELECT pg_try_advisory_lock(12345)").Single();

        private void UnlockDatabase(NpgsqlConnection connection) => connection.Execute(@"SELECT pg_advisory_unlock(12345)");

        private int GetInstalledVersion(NpgsqlConnection connection)
        {
            try
            {
                return connection.Query<int?>(@"SELECT version FROM schema").SingleOrDefault() ?? 1;
            }
            catch
            {
                return 1;
            }
        }

        private void TryCreateSchema(NpgsqlConnection connection)
        {
            try
            {
                connection.Execute($@"CREATE SCHEMA {_schemaName}");
            }
            catch
            {
            }

            connection.Execute($@"SET search_path={_schemaName}");
        }

        private static IEnumerable<MigrationInfo> GetMigrations()
        {
            var version = 3;

            while (true)
            {
                var resourceName = $"Hangfire.PostgreSql.Schema.Install.v{version.ToString(CultureInfo.InvariantCulture)}.sql";
                var stringResource = ReadStringResource(resourceName);

                if (stringResource.HasValue)
                {
                    yield return new MigrationInfo { Version = version, Script = stringResource.Value };
                    version++;
                }
                else
                {
                    yield break;
                }
            }
        }

        private static Option<string> ReadStringResource(string resourceName)
        {
            var assembly = typeof(DatabaseInitializer).GetTypeInfo().Assembly;
            try
            {
                using (var stream = assembly.GetManifestResourceStream(resourceName))
                {
                    if (stream == null) return default(Option<string>);

                    using (var reader = new StreamReader(stream))
                    {
                        var value = reader.ReadToEnd();
                        return new Option<string>(value);
                    }
                }
            }
            catch
            {
                return default(Option<string>);
            }
        }
    }
}
