using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Hangfire.Dashboard;
using Hangfire.PostgreSql.Tests.Setup;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.PostgreSql.Tests.Integration
{
    public class PostgreSqlDashboardMetricsIntegrationTests : StorageContextBasedTests<PostgreSqlDashboardMetricsIntegrationTests>
    {
        [Theory]
        [MemberData(nameof(GetMetrics))]
        public void DashboardMetric_Returns_Value(DashboardMetric dashboardMetric)
        {
            var page = new TestPage(Storage);

            var metric = dashboardMetric.Func(page);
            
            Assert.NotEqual("???", metric.Value);
        }

        // ReSharper disable once MemberCanBePrivate.Global
        public static IEnumerable<object[]> GetMetrics()
        {
            yield return new object[] { PostgreSqlDashboardMetrics.MaxConnections };
            yield return new object[] { PostgreSqlDashboardMetrics.ConnectionUsageRatio };
            yield return new object[] { PostgreSqlDashboardMetrics.DistributedLocksCount };
            yield return new object[] { PostgreSqlDashboardMetrics.ActiveConnections };
            yield return new object[] { PostgreSqlDashboardMetrics.CacheHitsPerRead };
            yield return new object[] { PostgreSqlDashboardMetrics.PostgreSqlLocksCount };
            yield return new object[] { PostgreSqlDashboardMetrics.PostgreSqlServerVersion };
        }

        public PostgreSqlDashboardMetricsIntegrationTests(
            StorageContext<PostgreSqlDashboardMetricsIntegrationTests> storageContext,
            ITestOutputHelper testOutputHelper)
            : base(storageContext, testOutputHelper) { }

        private class TestPage : RazorPage
        {
            public TestPage(PostgreSqlStorage postgreSqlStorage)
            {
                var methods = GetType()
                    .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic);
                
                var method = methods
                    .Single(mi =>
                        mi.Name == nameof(Assign) &&
                        mi.GetParameters().Length == 1 &&
                        mi.GetParameters()[0].ParameterType == typeof(DashboardContext));

                var context = new TestContext(postgreSqlStorage, new DashboardOptions());
                method.Invoke(this, new object[] { context });
            }

            public override void Execute() { }
        }

        private class TestContext : DashboardContext
        {
            public TestContext(JobStorage storage, DashboardOptions options)
                : base(storage, options) { }
        }
    }
}
