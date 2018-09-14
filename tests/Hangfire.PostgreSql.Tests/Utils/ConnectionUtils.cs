using System;
using System.Threading;
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

        private static readonly Lazy<IConnectionProvider> LazyConnectionProvider = new Lazy<IConnectionProvider>(() =>
        {
            var connectionString = GetConnectionString();
            var builder = new NpgsqlConnectionStringBuilder(connectionString);

            if (builder.Pooling)
            {
                return new NpgsqlConnectionProvider(connectionString);
            }
            else
            {
                return new DefaultConnectionProvider(connectionString);
            }
        }, LazyThreadSafetyMode.ExecutionAndPublication);

        public static IConnectionProvider GetConnectionProvider() => LazyConnectionProvider.Value;

        public static NpgsqlConnection CreateNpgConnection() => new NpgsqlConnection(GetConnectionString());
    }
}
