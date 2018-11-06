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
                    string schemaName = "hangfire_tests_" + Guid.NewGuid().ToString("N");

                    var databaseInitializer = new DatabaseInitializer(ConnectionUtils.GetConnectionProvider(), schemaName);

                    databaseInitializer.Initialize();

                    connectionHolder.Connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
                }
            });

            Assert.Null(ex);
        }

        [Fact]
        public void UpdateSchemaShouldNotThrowAnException()
        {
            var schemaName = "hangfire_tests_" + Guid.NewGuid().ToString("N");
            try
            {
                var databaseInitializer = new DatabaseInitializer(ConnectionUtils.GetConnectionProvider(), schemaName);

                // INIT DB
                databaseInitializer.Initialize();

                // TRY TO UPDATE
                databaseInitializer.Initialize();
            }
            finally
            {
                using (var connectionHolder = ConnectionUtils.GetConnectionProvider().AcquireConnection())
                {
                    connectionHolder.Connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
                }
            }
        }
    }
}
