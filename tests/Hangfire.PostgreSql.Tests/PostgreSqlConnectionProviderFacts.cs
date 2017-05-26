using System;
using Hangfire.PostgreSql.Tests.Utils;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlConnectionProviderFacts
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsNull()
        {
            var options = new PostgreSqlStorageOptions();
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlConnectionProvider(null, options));
            Assert.Equal("connectionString", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsInvalid()
        {
            var options = new PostgreSqlStorageOptions();
            var exception = Assert.Throws<ArgumentException>(
                () => new PostgreSqlConnectionProvider("testtest", options));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlConnectionProvider(ConnectionUtils.GetConnectionString(), null));
            Assert.Equal("options", exception.ParamName);
        }

        [Fact]
        public void AcquireConnection_CreatesOneConnection()
        {
            var provider = CreateProvider(1);

            using (var connectionHolder = provider.AcquireConnection())
            {
                Assert.Equal(1, provider.ActiveConnections);
            }
        }

        [Fact]
        public void AcquireConnection_CanReuseConnection()
        {
            var provider = CreateProvider(1);

            using (var connectionHolder = provider.AcquireConnection())
            {
                Assert.Equal(1, provider.ActiveConnections);
            }

            using (var connectionHolder2 = provider.AcquireConnection())
            {
                Assert.Equal(1, provider.ActiveConnections);
            }
        }

        [Fact]
        public void Dtor_ReleasesConnections()
        {
            var connectionsCount = 10;
            var provider = CreateProvider(connectionsCount);

            for (var i = 0; i < connectionsCount; i++)
            {
                provider.AcquireConnection();
            }

            provider.Dispose();

            Assert.Equal(0, provider.ActiveConnections);
        }

        private static PostgreSqlConnectionProvider CreateProvider(int connectionsCount)
        {
            return new PostgreSqlConnectionProvider(ConnectionUtils.GetConnectionString(), new PostgreSqlStorageOptions { ConnectionsCount = connectionsCount });
        }
    }
}
