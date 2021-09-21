using System;
using Hangfire.PostgreSql.Connectivity;

namespace Hangfire.PostgreSql.Tests.Setup
{
    internal static class ConnectivityUtilities
    {
        private const string ConnectionStringVariableName = "Hangfire_PostgreSql_ConnectionString";

        private const string DefaultConnectionString =
            @"Server=localhost;Port=5432;Database=hangfire_tests;User Id=postgres;Password=password;Search Path=hangfire_tests;";

        public static string GetConnectionString()
            => Environment.GetEnvironmentVariable(ConnectionStringVariableName) ?? DefaultConnectionString;

        public static NpgsqlConnectionProvider CreateNpgsqlConnectionProvider()
            => new(new DefaultConnectionBuilder(GetConnectionString()));
    }
}
