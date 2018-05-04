using System;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlInstallerFacts
    {
        [Fact]
        public void InstallingSchemaShouldNotThrowAnException()
        {
            var ex = Record.Exception(() =>
            {
                UseConnection((provider, connection) =>
                {
                    string schemaName = "hangfire_tests_" + Guid.NewGuid().ToString().Replace("-", "_").ToLower();

                    DatabaseInitializer.Initialize(connection, schemaName);

                    connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
                });
            });

            Assert.Null(ex);
        }

        private static void UseConnection(Action<IConnectionProvider, NpgsqlConnection> action)
        {
            var provider = ConnectionUtils.CreateConnection();
            using (var connection = provider.AcquireConnection())
            {
                action(provider, connection.Connection);
            }
        }
    }
}
