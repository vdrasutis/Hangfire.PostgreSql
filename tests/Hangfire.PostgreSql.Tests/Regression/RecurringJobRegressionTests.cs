using System.Threading;
using Hangfire.PostgreSql.Tests.Setup;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.PostgreSql.Tests.Regression
{
    public class RecurringJobRegressionTests : StorageContextBasedTests<RecurringJobRegressionTests>
    {
        // test method, do not enable warning
#pragma warning disable xUnit1013
        // ReSharper disable once MemberCanBePrivate.Global
        public static void TestMethod() { }
#pragma warning restore xUnit1013
        
        // https://github.com/ahydrax/Hangfire.PostgreSql/issues/12
        [Fact]
        public void ParallelAddingSameRecurringJob_DoesntThrowException()
        {
            var recurringJobManager = new RecurringJobManager(Storage);

            ThreadStart action = () => recurringJobManager.AddOrUpdate("TEST_JOB", () => TestMethod(), Cron.Daily());

            var thread1 = new Thread(action);
            var thread2 = new Thread(action);

            thread1.Start();
            thread2.Start();

            thread1.Join();
            thread2.Join();
        }
        
        public RecurringJobRegressionTests(StorageContext<RecurringJobRegressionTests> storageContext, ITestOutputHelper testOutputHelper) : base(storageContext, testOutputHelper) { }
    }
}
