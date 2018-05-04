using System;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlDistributedLockFacts
    {
        private readonly TimeSpan _timeout = TimeSpan.FromSeconds(5);

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNullOrEmpty()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new DistributedLock("", _timeout, new Mock<IConnectionProvider>().Object));

            Assert.Equal("resource", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new DistributedLock("hello", _timeout, null));

            Assert.Equal("connectionProvider", exception.ParamName);
        }
        
        [Fact, CleanDatabase]
        public void Ctor_AcquiresExclusiveApplicationLock()
        {
            UseConnection((provider, connection) =>
            {
                // ReSharper disable once UnusedVariable
                var distributedLock = new DistributedLock("hello", _timeout, provider);

                var lockCount = connection.Query<long>(
                    @"select count(*) from ""lock"" where ""resource"" = @resource",
                    new { resource = "hello" }).Single();

                Assert.Equal(1, lockCount);
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfLockCanNotBeGranted()
        {
            var releaseLock = new ManualResetEventSlim(false);
            var lockAcquired = new ManualResetEventSlim(false);

            var thread = new Thread(
                () => UseConnection((provider, connection) =>
                {
                    using (new DistributedLock("exclusive", _timeout, provider))
                    {
                        lockAcquired.Set();
                        releaseLock.Wait();
                    }
                }));
            thread.Start();

            lockAcquired.Wait();

            UseConnection((provider, connection) =>
                Assert.Throws<DistributedLockException>(
                    () => new DistributedLock("exclusive", _timeout, provider)));

            releaseLock.Set();
            thread.Join();
        }

        [Fact, CleanDatabase]
        public void Dispose_ReleasesExclusiveApplicationLock()
        {
            UseConnection((provider, connection) =>
            {
                var distributedLock = new DistributedLock("hello", _timeout, provider);
                distributedLock.Dispose();

                var lockCount = connection.Query<long>(
                    @"select count(*) from """ + GetSchemaName() + @""".""lock"" where ""resource"" = @resource",
                    new { resource = "hello" }).Single();

                Assert.Equal(0, lockCount);
            });
        }

        private void UseConnection(Action<IConnectionProvider, NpgsqlConnection> action)
        {
            var provider = ConnectionUtils.GetConnectionProvider();
            using (var connection = provider.AcquireConnection())
            {
                action(provider, connection.Connection);
            }
        }

        private static string GetSchemaName() => ConnectionUtils.GetSchemaName();
    }
}
