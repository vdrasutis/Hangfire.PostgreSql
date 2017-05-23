using System;
using Dapper;
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

                    PostgreSqlObjectsInstaller.Install(connection, schemaName);

                    connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
                });
            });

            Assert.Null(ex);
        }

        private static void UseConnection(Action<IPostgreSqlConnectionProvider, NpgsqlConnection> action)
        {
            var provider = ConnectionUtils.CreateConnection();
            using (var connection = provider.AcquireConnection())
            {
                action(provider, connection.Connection);
            }
        }
    }
}
