using System;
using System.Linq;
using Hangfire.PostgreSql.Locking;
using Hangfire.PostgreSql.Maintenance;
using Hangfire.PostgreSql.Storage;
using Hangfire.PostgreSql.Tests.Setup;
using Xunit;

namespace Hangfire.PostgreSql.Tests.Unit
{
    public class PostgreSqlStorageUnitTests
    {
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlStorageUnitTests()
        {
            _options = new PostgreSqlStorageOptions { PrepareSchemaIfNecessary = false };
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlStorage(connectionString: null));

            Assert.Equal("connectionString", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionBuilderIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlStorage(connectionBuilder: null));

            Assert.Equal("connectionBuilder", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () =>
                {
                    using var storage = new PostgreSqlStorage(ConnectivityUtilities.GetConnectionString(), null);
                });

            Assert.Equal("options", exception.ParamName);
        }

        [Fact]
        public void GetMonitoringApi_ReturnsNonNullInstance()
        {
            using var storage = CreateStorage();
            var api = storage.GetMonitoringApi();
            Assert.NotNull(api);
        }

        [Fact]
        public void GetConnection_ReturnsNonNullInstance()
        {
            using var storage = CreateStorage();
            using var connection = (StorageConnection)storage.GetConnection();
            Assert.NotNull(connection);
        }

        [Fact]
        public void GetComponents_ReturnsAllNeededComponents()
        {
            using var storage = CreateStorage();

            var components = storage.GetComponents();

            var componentTypes = components.Select(x => x.GetType()).ToArray();
            Assert.Contains(typeof(LockService), componentTypes);
            Assert.Contains(typeof(ExpirationManager), componentTypes);
            Assert.Contains(typeof(CountersAggregationManager), componentTypes);
        }

        private PostgreSqlStorage CreateStorage() => new(ConnectivityUtilities.GetConnectionString(), _options);
    }
}
