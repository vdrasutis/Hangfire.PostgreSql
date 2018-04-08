using System;
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
            @"Server=127.0.0.1;Port=5432;Database=postgres;User Id=postgres;Password=password;Pooling=false;Search Path=hangfire";

        private static readonly IPostgreSqlConnectionProvider ConnectionProvider = new PostgreSqlConnectionProvider(
            GetConnectionString(),
            new PostgreSqlStorageOptions { SchemaName = DefaultSchemaName });

        public static string GetDatabaseName() => Environment.GetEnvironmentVariable(DatabaseVariable) ?? DefaultDatabaseName;

        public static string GetSchemaName() => Environment.GetEnvironmentVariable(SchemaVariable) ?? DefaultSchemaName;

        public static string GetMasterConnectionString() => string.Format(GetConnectionStringTemplate(), MasterDatabaseName);

        public static string GetConnectionString() => string.Format(GetConnectionStringTemplate(), GetDatabaseName());

        private static string GetConnectionStringTemplate() => Environment.GetEnvironmentVariable(ConnectionStringTemplateVariable) ?? DefaultConnectionStringTemplate;

        public static IPostgreSqlConnectionProvider CreateConnection() => ConnectionProvider;

        public static NpgsqlConnection CreateNpgConnection() => new NpgsqlConnection(GetConnectionString());
    }
}
