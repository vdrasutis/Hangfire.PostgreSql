using System;
using Dapper;
using Hangfire.PostgreSql.Tests.Utils;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class DatabaseInitializerFacts
    {
        [Fact]
        public void InstallingSchemaShouldNotThrowAnException()
        {
            var ex = Record.Exception(() =>
            {
                using (var connectionHolder = ConnectionUtils.GetConnectionProvider().AcquireConnection())
                {
                    string schemaName = "hangfire_tests_" + Guid.NewGuid().ToString().Replace("-", "_").ToLower();

                    var databaseInitializer = new DatabaseInitializer(ConnectionUtils.GetConnectionProvider(), schemaName);

                    databaseInitializer.Initialize();

                    connectionHolder.Connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
                }
            });

            Assert.Null(ex);
        }
    }
}
