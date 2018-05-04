using System;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlConnectionProviderFacts
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new DefaultConnectionProvider(null));
            Assert.Equal("connectionString", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsInvalid()
        {
            var exception = Assert.Throws<ArgumentException>(
                () => new DefaultConnectionProvider("testtest"));
        }

        [Fact]
        public void AcquireConnection_CreatesOneConnection()
        {
            var provider = CreateProvider();

            using (var connectionHolder = provider.AcquireConnection())
            {
                Assert.Equal(1, provider.ActiveConnections);
            }
        }

        [Fact]
        public void AcquireConnection_CanReuseConnection()
        {
            var provider = CreateProvider();

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
            var provider = CreateProvider();

            for (var i = 0; i < connectionsCount; i++)
            {
                provider.AcquireConnection();
            }

            provider.Dispose();

            Assert.Equal(0, provider.ActiveConnections);
        }

        private static DefaultConnectionProvider CreateProvider()
        {
            return new DefaultConnectionProvider(ConnectionUtils.GetConnectionString());
        }
    }
}
