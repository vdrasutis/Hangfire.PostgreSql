using System;
using System.Threading;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Setup;
using Hangfire.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.PostgreSql.Tests.Integration
{
    public class LockAcquirerIntegrationTests : StorageContextBasedTests<LockAcquirerIntegrationTests>
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNullOrEmpty()
        {
            var exception = Assert.Throws<ArgumentException>(
                () => LockService.AcquireLock("", TimeSpan.FromSeconds(1)));

            Assert.Equal("resource", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenTimeoutIsNegativeValue()
        {
            var exception = Assert.Throws<ArgumentException>(
                () => LockService.AcquireLock("hello", TimeSpan.FromSeconds(-1)));

            Assert.Equal("timeout", exception.ParamName);
        }

        [Fact]
        public void Ctor_AcquiresExclusiveApplicationLock()
        {
            // Arrange
            using var _ = LockService.AcquireLock("hello", TimeSpan.FromSeconds(1));

            // Act
            var lockCount = ConnectionProvider.FetchScalar<long>(
                @"select count(*) from lock where resource = @resource;",
                new { resource = "hello" });

            // Assert
            Assert.Equal(1, lockCount);
        }

        [Fact]
        public void Ctor_ThrowsAnException_IfLockCanNotBeGranted()
        {
            // Arrange
            var timeout = TimeSpan.FromSeconds(3);
            var releaseLock = new ManualResetEventSlim(false);
            var lockAcquired = new ManualResetEventSlim(false);
            var thread = new Thread(
                () =>
                {
                    using var _ = LockService.AcquireLock("exclusive", timeout);
                    lockAcquired.Set();
                    releaseLock.Wait();
                });

            // Act
            thread.Start();
            lockAcquired.Wait();

            // Assert
            Assert.Throws<DistributedLockTimeoutException>(
                () => LockService.AcquireLock("exclusive", timeout));

            releaseLock.Set();
            thread.Join();
        }

        [Fact]
        public void Dispose_ReleasesExclusiveApplicationLock()
        {
            // Arrange
            var distributedLock = LockService.AcquireLock("hello", TimeSpan.FromSeconds(1));

            // Act
            distributedLock.Dispose();

            // Assert
            var lockCount = ConnectionProvider.FetchScalar<long>(
                @"select count(*) from lock where resource = @resource;",
                new { resource = "hello" });

            Assert.Equal(0, lockCount);
        }

        [Fact]
        public void Dispose_ThrowsExceptionWhenLockWasDeletedExternally()
        {
            // Arrange
            var distributedLock = LockService.AcquireLock("hello", TimeSpan.FromSeconds(1));

            // Act
            ConnectionProvider.Execute("delete from lock;");

            // Assert
            Assert.Throws<DistributedLockException>(() => distributedLock.Dispose());
        }

        public LockAcquirerIntegrationTests(StorageContext<LockAcquirerIntegrationTests> storageContext, ITestOutputHelper testOutputHelper) :
            base(storageContext, testOutputHelper) { }
    }
}
