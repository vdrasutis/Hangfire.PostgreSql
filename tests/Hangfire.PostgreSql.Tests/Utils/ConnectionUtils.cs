using System;
using System.Threading;
using Npgsql;

namespace Hangfire.PostgreSql.Tests.Utils
{
    internal static class ConnectionUtils
    {
        private const string DatabaseVariable = "Hangfire_PostgreSql_DatabaseName";
        private const string SchemaVariable = "Hangfire_PostgreSql_SchemaName";

        private const string ConnectionStringTemplateVariable = "Hangfire_PostgreSql_ConnectionStringTemplate";

        private const string MasterDatabaseName = "postgres";
        private const string DefaultDatabaseName = @"hangfire_tests";
        private const string DefaultSchemaName = @"hangfire";

        private const string DefaultConnectionStringTemplate =
            @"Server=127.0.0.1;Port=5432;Database=postgres;User Id=postgres;Password=password;Pooling=false";

        private static readonly Lazy<IPostgreSqlConnectionProvider> LazyProvider
            = new Lazy<IPostgreSqlConnectionProvider>(
                () => new PostgreSqlConnectionProvider(GetConnectionString(),
                    new PostgreSqlStorageOptions()), LazyThreadSafetyMode.ExecutionAndPublication);

        public static string GetDatabaseName()
        {
            return Environment.GetEnvironmentVariable(DatabaseVariable) ?? DefaultDatabaseName;
        }

        public static string GetSchemaName()
        {
            return Environment.GetEnvironmentVariable(SchemaVariable) ?? DefaultSchemaName;
        }

        public static string GetMasterConnectionString()
        {
            return string.Format(GetConnectionStringTemplate(), MasterDatabaseName);
        }

        public static string GetConnectionString()
        {
            return string.Format(GetConnectionStringTemplate(), GetDatabaseName());
        }

        private static string GetConnectionStringTemplate()
        {
            return Environment.GetEnvironmentVariable(ConnectionStringTemplateVariable)
                   ?? DefaultConnectionStringTemplate;
        }

        public static IPostgreSqlConnectionProvider CreateConnection()
        {
            return LazyProvider.Value;
        }

        public static NpgsqlConnection CreateNpgConnection()
        {
            return new NpgsqlConnection(GetConnectionString());
        }

        // TODO unify
        public static void UseConnection(Action<IPostgreSqlConnectionProvider, NpgsqlConnection> action)
        {
            using (var connection = LazyProvider.Value.AcquireConnection())
            {
                action(LazyProvider.Value, connection.Connection);
            }
        }
    }
}
