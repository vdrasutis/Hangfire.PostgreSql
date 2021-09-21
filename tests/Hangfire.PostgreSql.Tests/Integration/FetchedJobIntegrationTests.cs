using System;
using System.Data;
using System.Linq;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Setup;
using Npgsql;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.PostgreSql.Tests.Integration
{
    public class FetchedJobIntegrationTests : StorageContextBasedTests<FetchedJobIntegrationTests>
    {
        [Fact]
        public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
        {
            UseConnection((provider, connection) =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, "1", "default");
                var processingJob = new FetchedJob(provider, id, "1", "default");

                // Act
                processingJob.RemoveFromQueue();

                // Assert
                var count = connection.Query<long>(@"select count(*) from jobqueue")
                    .Single();
                Assert.Equal(0, count);
            });
        }

        [Fact]
        public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
        {
            UseConnection((provider, connection) =>
            {
                // Arrange
                CreateJobQueueRecord(connection, "1", "default");
                CreateJobQueueRecord(connection, "1", "critical");
                CreateJobQueueRecord(connection, "2", "default");

                var fetchedJob = new FetchedJob(provider, 999, "1", "default");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                var count = connection.Query<long>(@"select count(*) from jobqueue")
                    .Single();
                Assert.Equal(3, count);
            });
        }

        [Fact]
        public void Requeue_SetsFetchedAtValueToNull()
        {
            UseConnection((provider, connection) =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, "1", "default");
                var processingJob = new FetchedJob(provider, id, "1", "default");

                // Act
                processingJob.Requeue();

                // Assert
                var record = connection.Query(@"select * from jobqueue").Single();
                Assert.Null(record.FetchedAt);
            });
        }

        [Fact]
        public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
        {
            UseConnection((provider, connection) =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, "1", "default");
                var processingJob = new FetchedJob(provider, id, "1", "default");

                // Act
                processingJob.Dispose();

                // Assert
                var record = connection.Query(@"select * from jobqueue").Single();
                Assert.Null(record.fetchedat);
            });
        }

        private static int CreateJobQueueRecord(IDbConnection connection, string jobId, string queue)
        {
            const string arrangeSql = @"
insert into jobqueue (jobid, queue, fetchedat)
values (@id, @queue, now() at time zone 'utc') returning ""id""";

            return
                (int)
                connection.Query(arrangeSql,
                        new { id = JobId.ToLong(jobId), queue = queue })
                    .Single()
                    .id;
        }

        private void UseConnection(Action<IConnectionProvider, NpgsqlConnection> action)
        {
            using var connectionHolder = ConnectionProvider.AcquireConnection();
            action(ConnectionProvider, connectionHolder.Connection);
        }

        public FetchedJobIntegrationTests(StorageContext<FetchedJobIntegrationTests> storageContext, ITestOutputHelper testOutputHelper) : base(storageContext, testOutputHelper) { }
    }
}
