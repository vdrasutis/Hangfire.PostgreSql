using System;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Locking;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.PostgreSql.Tests.Setup
{
    public abstract class StorageContextBasedTests<T> : IClassFixture<StorageContext<T>>, IDisposable
        where T : StorageContextBasedTests<T>
    {
        protected StorageContext<T> StorageContext { get; }
        protected ITestOutputHelper TestOutputHelper { get; }

        internal PostgreSqlStorage Storage => StorageContext.Storage;

        internal IConnectionProvider ConnectionProvider => StorageContext.Storage.ConnectionProvider;

        internal ILockService LockService => StorageContext.Storage.LockService;

        protected StorageContextBasedTests(StorageContext<T> storageContext, ITestOutputHelper testOutputHelper)
        {
            StorageContext = storageContext;
            TestOutputHelper = testOutputHelper;
        }

        public void Dispose()
        {
            StorageContext.TruncateTables();
            TestOutputHelper.WriteLine("db cleaned up");
        }
    }
}
