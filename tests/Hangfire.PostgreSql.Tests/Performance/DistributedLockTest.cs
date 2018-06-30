using System;
using System.Linq;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Xunit;

namespace Hangfire.PostgreSql.Tests.Performance
{
    public class DistributedLockTest
    {
        [Fact(Skip = "Use only for profiling/benchmarking. Not stable"), CleanDatabase]
        public void Perf_AcquiringLock()
        {
            var connectionProvider = ConnectionUtils.GetConnectionProvider();

            var threads = Enumerable.Range(1, 100).AsParallel()
                .WithDegreeOfParallelism(100)
                .WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                .Select(x => AcquireLock(connectionProvider))
                .Sum();

            Assert.Equal(100, threads);
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
            catch (DistributedLockException)
            {
                return 0;
            }
        }
    }
}
