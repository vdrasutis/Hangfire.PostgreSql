using System;
using Dapper;
using Hangfire.PostgreSql.Tests.Setup;
using Xunit;

namespace Hangfire.PostgreSql.Tests.Integration
{
    public class DatabaseInitializerIntegrationTests : IClassFixture<NpgsqlConnectionProviderContext>
    {
        [Fact]
        public void InstallingSchemaShouldNotThrowAnException()
        {
            var ex = Record.Exception(() =>
            {
                using var connectionHolder = _npgsqlConnectionProviderContext.ConnectionProvider.AcquireConnection();
                var schemaName = "hangfire_tests_db_initializer_" + Guid.NewGuid().ToString("N");

                var databaseInitializer =
                    new DatabaseInitializer(_npgsqlConnectionProviderContext.ConnectionProvider, schemaName);

                databaseInitializer.Initialize();

                connectionHolder.Connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
            });

            Assert.Null(ex);
        }

        [Fact]
        public void UpdateSchemaShouldNotThrowAnException()
        {
            var schemaName = "hangfire_tests_db_initializer_" + Guid.NewGuid().ToString("N");

            try
            {
                var databaseInitializer =
                    new DatabaseInitializer(_npgsqlConnectionProviderContext.ConnectionProvider, schemaName);

                // INIT DB
                databaseInitializer.Initialize();

                // TRY TO UPDATE
                databaseInitializer.Initialize();
            }
            finally
            {
                using var connectionHolder = _npgsqlConnectionProviderContext.ConnectionProvider.AcquireConnection();
                connectionHolder.Connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
            }
        }

        private readonly NpgsqlConnectionProviderContext _npgsqlConnectionProviderContext;

        public DatabaseInitializerIntegrationTests(NpgsqlConnectionProviderContext npgsqlConnectionProviderContext)
        {
            _npgsqlConnectionProviderContext = npgsqlConnectionProviderContext;
        }
    }
}
