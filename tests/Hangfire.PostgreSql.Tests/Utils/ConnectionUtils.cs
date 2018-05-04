using System;
using Hangfire.PostgreSql.Connectivity;
using Npgsql;

namespace Hangfire.PostgreSql.Tests.Utils
{
    internal static class ConnectionUtils
    {
        private const string ConnectionStringVariableName = "Hangfire_PostgreSql_ConnectionString";

        private const string DefaultConnectionString =
            @"Server=localhost;Port=5432;Database=hangfire_tests;User Id=postgres;Password=password;Search Path=hangfire";

        public static string GetConnectionString() => Environment.GetEnvironmentVariable(ConnectionStringVariableName) ?? DefaultConnectionString;

        private static readonly IConnectionProvider ConnectionProvider = new DefaultConnectionProvider(GetConnectionString());

        public static string GetDatabaseName()
        {
            var builder = new NpgsqlConnectionStringBuilder(GetConnectionString());
            return builder.Database;
        }

        public static string GetSchemaName()
        {
            var builder = new NpgsqlConnectionStringBuilder(GetConnectionString());
            return builder.SearchPath;
        }

        public static IConnectionProvider CreateConnection() => ConnectionProvider;

        public static NpgsqlConnection CreateNpgConnection() => new NpgsqlConnection(GetConnectionString());
    }
}
