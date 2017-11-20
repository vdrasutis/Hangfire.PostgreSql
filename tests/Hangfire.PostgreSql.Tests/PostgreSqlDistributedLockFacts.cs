using System;
using System.Data;
using System.Linq;
using System.Threading;
using Dapper;
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
            var options = new PostgreSqlStorageOptions();

            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlDistributedLock("", _timeout, new Mock<IPostgreSqlConnectionProvider>().Object, options));

            Assert.Equal("resource", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var options = new PostgreSqlStorageOptions();

            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlDistributedLock("hello", _timeout, null, options));

            Assert.Equal("connectionProvider", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlDistributedLock("hi", _timeout, new Mock<IPostgreSqlConnectionProvider>().Object, null));

            Assert.Equal("options", exception.ParamName);
        }


        [Fact, CleanDatabase]
        public void Ctor_AcquiresExclusiveApplicationLock()
        {
            var options = new PostgreSqlStorageOptions
            {
                SchemaName = GetSchemaName()
            };

            UseConnection((provider, connection) =>
            {
                // ReSharper disable once UnusedVariable
                var distributedLock = new PostgreSqlDistributedLock("hello", _timeout, provider, options);

                var lockCount = connection.Query<long>(
                    @"select count(*) from """ + GetSchemaName() + @""".""lock"" where ""resource"" = @resource",
                    new { resource = "hello" }).Single();

                Assert.Equal(1, lockCount);
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_AcquiresExclusiveApplicationLock_WhenDeadlockIsOccured()
        {
            var options = new PostgreSqlStorageOptions
            {
                SchemaName = GetSchemaName(),
                DistributedLockTimeout = TimeSpan.FromSeconds(5)
            };

            UseConnection((provider, connection) =>
            {
                // Arrange
                var timeout = TimeSpan.FromSeconds(7);
                var resourceName = "hello";
                connection.Execute(
                    $@"INSERT INTO ""{GetSchemaName()}"".""lock"" VALUES ('{resourceName}', 0, '{DateTime.UtcNow}')");

                // Act
                var distributedLock = new PostgreSqlDistributedLock(resourceName, timeout, provider, options);

                // Assert
                Assert.True(distributedLock != null);
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfLockCanNotBeGranted()
        {
            var options = new PostgreSqlStorageOptions
            {
                SchemaName = GetSchemaName()
            };

            var releaseLock = new ManualResetEventSlim(false);
            var lockAcquired = new ManualResetEventSlim(false);

            var thread = new Thread(
                () => UseConnection((provider, connection) =>
                {
                    using (new PostgreSqlDistributedLock("exclusive", _timeout, provider, options))
                    {
                        lockAcquired.Set();
                        releaseLock.Wait();
                    }
                }));
            thread.Start();

            lockAcquired.Wait();

            UseConnection((provider, connection) =>
                Assert.Throws<PostgreSqlDistributedLockException>(
                    () => new PostgreSqlDistributedLock("exclusive", _timeout, provider, options)));

            releaseLock.Set();
            thread.Join();
        }

        [Fact, CleanDatabase]
        public void Dispose_ReleasesExclusiveApplicationLock()
        {
            var options = new PostgreSqlStorageOptions
            {
                SchemaName = GetSchemaName()
            };

            UseConnection((provider, connection) =>
            {
                var distributedLock = new PostgreSqlDistributedLock("hello", _timeout, provider, options);
                distributedLock.Dispose();

                var lockCount = connection.Query<long>(
                    @"select count(*) from """ + GetSchemaName() + @""".""lock"" where ""resource"" = @resource",
                    new { resource = "hello" }).Single();

                Assert.Equal(0, lockCount);
            });
        }

        private void UseConnection(Action<IPostgreSqlConnectionProvider, NpgsqlConnection> action)
        {
            var provider = ConnectionUtils.CreateConnection();
            using (var connection = provider.AcquireConnection())
            {
                action(provider, connection.Connection);
            }
        }

        private static string GetSchemaName()
        {
            return ConnectionUtils.GetSchemaName();
        }
    }
}
