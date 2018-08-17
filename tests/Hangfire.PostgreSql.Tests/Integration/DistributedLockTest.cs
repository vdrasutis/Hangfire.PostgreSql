using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.PostgreSql.Tests.Performance
{
    public class DistributedLockTest
    {
        [Fact(Skip = "Run only local."), CleanDatabase]
        public void Perf_AcquiringLock()
        {
            const int concurrentQueries = 1000;
            var connectionProvider = ConnectionUtils.GetConnectionProvider();

            var threads = Enumerable.Range(1, concurrentQueries).AsParallel()
                .WithDegreeOfParallelism(Math.Min(concurrentQueries, 511)) // 511 is max for that method
                .WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                .AsUnordered()
                .Select(x => AcquireLock(connectionProvider))
                .Sum();

            Assert.Equal(concurrentQueries, threads);
        }

        private static int AcquireLock(IConnectionProvider connectionProvider)
        {
            try
            {
                using (var @lock = new DistributedLock("hello", TimeSpan.FromSeconds(1), connectionProvider))
                {
                    return 1;
                }
            }
            catch (DistributedLockTimeoutException)
            {
                return 0;
            }
        }

        [Fact(Skip = "Run only local."), CleanDatabase]
        public void Ctor_ActuallyGrantsExclusiveLock()
        {
            const int numberOfParallelJobs = 1000;
            var connectionProvider = ConnectionUtils.GetConnectionProvider();
            var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = numberOfParallelJobs };
            var i = 0;

            Parallel.For(0, numberOfParallelJobs, parallelOptions, _ =>
            {
                using (new DistributedLock("increment_test", TimeSpan.FromSeconds(1), connectionProvider))
                {
                    // prevent compiler/jit from reordering
                    var temp = Volatile.Read(ref i);
                    Volatile.Write(ref i, temp + 1);
                }
            });

            Assert.Equal(numberOfParallelJobs, i);
        }
    }
}
