using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.PostgreSql.Locking;
using Hangfire.PostgreSql.Tests.Setup;
using Hangfire.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.PostgreSql.Tests.Regression
{
    public class LockServiceRegressionTests : StorageContextBasedTests<LockServiceRegressionTests>
    {
        public LockServiceRegressionTests(StorageContext<LockServiceRegressionTests> storageContext, ITestOutputHelper testOutputHelper) : base(
            storageContext, testOutputHelper) { }

        [Fact]
        public void Perf_AcquiringLock_SameResource()
        {
            var lockTimeout = TimeSpan.FromSeconds(30);
            var concurrentQueries = Environment.ProcessorCount * 2;

            var threads = RunConcurrent(concurrentQueries,
                lockTimeout,
                StorageContext.Storage.LockService,
                _ => "oneLockToRuleThemAll");

            Assert.Equal(concurrentQueries, threads);
        }

        [Fact]
        public void Perf_AcquiringLock_DifferentResources()
        {
            var lockTimeout = TimeSpan.Zero;
            var concurrentQueries = Environment.ProcessorCount * 2;

            var successFullThreads = RunConcurrent(concurrentQueries,
                lockTimeout,
                StorageContext.Storage.LockService,
                x => x.ToString());

            Assert.Equal(concurrentQueries, successFullThreads);
        }

        [Fact]
        public void Ctor_ActuallyGrantsExclusiveLock()
        {
            var numberOfParallelJobs = Environment.ProcessorCount * 2;
            var lockService = StorageContext.Storage.LockService;
            var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = numberOfParallelJobs };
            var i = 0;

            Parallel.For(0, numberOfParallelJobs, parallelOptions, _ =>
            {
                using (lockService.AcquireLock("increment_test", TimeSpan.FromSeconds(30)))
                {
                    // prevent compiler/jit from reordering
                    var temp = Volatile.Read(ref i);
                    Volatile.Write(ref i, temp + 1);
                }
            });

            Assert.Equal(numberOfParallelJobs, i);
        }

        private static int RunConcurrent(int concurrentQueries,
            TimeSpan lockTimeout,
            ILockAcquirer lockService,
            Func<int, string> lockNameSelector)
            => Enumerable.Range(1, concurrentQueries)
                .AsParallel()
                .WithDegreeOfParallelism(Math.Min(concurrentQueries, 511)) // 511 is max for that method
                .WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                .AsUnordered()
                .Select(x => AcquireLock(lockNameSelector(x), lockTimeout, lockService))
                .Sum();

        private static int AcquireLock(string resourceName, TimeSpan lockTimeout, ILockAcquirer lockService)
        {
            try
            {
                using (lockService.AcquireLock(resourceName, lockTimeout))
                {
                    return 1;
                }
            }
            catch (DistributedLockTimeoutException)
            {
                return 0;
            }
        }
    }
}
