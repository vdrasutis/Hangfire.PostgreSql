using System;
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
                var lockTaken = LockDatabase(connection, _schemaName);
                if (!lockTaken) return;

                TryCreateSchema(connection);
                var installedVersion = GetInstalledVersion(connection);
                var availableMigrations = GetMigrations().Where(x => x.Version > installedVersion).ToArray();
                if (availableMigrations.Length == 0) return;

                using (var transaction = connection.BeginTransaction(IsolationLevel.Serializable))
                {
                    var lastMigration = default(MigrationInfo);
                    foreach (var migration in availableMigrations)
                    {
                        try
                        {
                            connection.Execute($@"set search_path={_schemaName}");
                            connection.Execute(migration.Script, transaction: transaction);
                        }
                        catch (Exception e)
                        {
                            var errorMessage = $"Error during installing v{migration.Version}";
                            Log.ErrorException(errorMessage, e);
                            throw new ApplicationException(errorMessage, e);
                        }

                        lastMigration = migration;
                        Log.Info($"Installing Hangfire SQL migration #{migration.Version}");
                    }

                    connection.Execute(
                        @"update schema set version = @version",
                        new { version = lastMigration.Version },
                        transaction);

                    transaction.Commit();
                }

                UnlockDatabase(connection, _schemaName);
            }

            Log.Info("Hangfire SQL objects installed.");
        }

        private bool LockDatabase(NpgsqlConnection connection, string lockId)
            => connection.Query<bool>(@$"select pg_try_advisory_lock(hashtext('{lockId}'))").Single();

        private void UnlockDatabase(NpgsqlConnection connection, string lockId)
            => connection.Execute(@$"select pg_advisory_unlock(hashtext('{lockId}'))");

        private int GetInstalledVersion(NpgsqlConnection connection)
        {
            try
            {
                connection.Execute($@"set search_path={_schemaName}");
                return connection.Query<int>(@"select version from schema").SingleOrDefault();
            }
            catch
            {
                return 1;
            }
        }

        private void TryCreateSchema(NpgsqlConnection connection)
        {
            connection.Execute($@"create schema if not exists {_schemaName}");
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
                    if (stream == null) return default;

                    using (var reader = new StreamReader(stream))
                    {
                        var value = reader.ReadToEnd();
                        return new Option<string>(value);
                    }
                }
            }
            catch
            {
                return default;
            }
        }
    }
}
