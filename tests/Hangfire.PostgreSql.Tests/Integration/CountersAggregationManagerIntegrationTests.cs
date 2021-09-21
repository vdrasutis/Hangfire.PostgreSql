using System;
using System.Threading;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Maintenance;
using Hangfire.PostgreSql.Tests.Setup;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.PostgreSql.Tests.Integration
{
    public class CountersAggregationManagerIntegrationTests : StorageContextBasedTests<CountersAggregationManagerIntegrationTests>
    {
        [Fact]
        public void Execute_Aggregates_CounterTable()
        {
            // Arrange
            const string createSql = @"insert into counter (key, value) values ('stats:succeeded', 1)";
            const int sum = 5;
            for (int i = 0; i < sum; i++)
            {
                ConnectionProvider.Execute(createSql);
            }

            // Act
            _manager.Execute(CancellationToken.None);

            // Assert
            const string checkQuery = @"select count(*), sum(value) from counter";
            var (recordsCount, recordsValue) = ConnectionProvider.Fetch<(long, long)>(checkQuery);
            Assert.Equal(1, recordsCount);
            Assert.Equal(sum, recordsValue);
        }

        private readonly CountersAggregationManager _manager;

        public CountersAggregationManagerIntegrationTests(StorageContext<CountersAggregationManagerIntegrationTests> storageContext, ITestOutputHelper testOutputHelper) : base(
            storageContext, testOutputHelper)
        {
            _manager = new CountersAggregationManager(ConnectionProvider, LockService, TimeSpan.FromMilliseconds(1));
        }
    }
}
