using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using Hangfire.PostgreSql.Tests.Setup;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.PostgreSql.Tests.Integration
{
    public class MonitoringApiIntegrationTests : StorageContextBasedTests<MonitoringApiIntegrationTests>
    {
        private readonly IMonitoringApi _monitoringApi;
        private readonly PostgreSqlStorage _storage;

        [Fact]
        public void ScheduledCount_ReturnsActualValue()
        {
            // Arrange
            var backgroundJobClient = new BackgroundJobClient(_storage);

            backgroundJobClient.Schedule(
                () => Worker.DoWork("hello"),
                TimeSpan.FromSeconds(1));
            backgroundJobClient.Schedule(
                () => Worker.DoWork("hello-2"),
                TimeSpan.FromSeconds(2));

            // Act
            var scheduledCount = _monitoringApi.ScheduledCount();

            // Assert
            Assert.Equal(2, scheduledCount);
        }

        [Fact]
        public void ScheduledJobs_ReturnsActualJobsList()
        {
            // Arrange
            var backgroundJobClient = new BackgroundJobClient(_storage);

            backgroundJobClient.Schedule(
                () => Worker.DoWork("hello"),
                TimeSpan.FromSeconds(1));
            backgroundJobClient.Schedule(
                () => Worker.DoWork("hello-2"),
                TimeSpan.FromSeconds(2));

            // Act
            var scheduledJobs = _monitoringApi.ScheduledJobs(0, 10);

            // Assert
            Assert.Equal(2, scheduledJobs.Count);
            Assert.All(scheduledJobs, x =>
            {
                var (key, value) = x;
                Assert.NotNull(key);
                Assert.True(value.InScheduledState);
                Assert.NotNull(value.ScheduledAt);
                Assert.NotNull(value.Job);
            });
        }

        [Fact]
        public void EnqueuedCount_ReturnsActualValue()
        {
            // Arrange
            var backgroundJobClient = new BackgroundJobClient(_storage);
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-2"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-3"));

            // Act
            var enqueuedCount = _monitoringApi.EnqueuedCount("default");

            // Assert
            Assert.Equal(3, enqueuedCount);
        }

        [Fact]
        public void EnqueuedJobs_ReturnsActualJobsList()
        {
            // Arrange
            var backgroundJobClient = new BackgroundJobClient(_storage);

            backgroundJobClient.Enqueue(() => Worker.DoWork("hello"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-2"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-3"));

            // Act
            var enqueuedJobs = _monitoringApi.EnqueuedJobs("default", 0, 10);

            // Assert
            Assert.Equal(3, enqueuedJobs.Count);
            Assert.All(enqueuedJobs, x =>
            {
                var (key, value) = x;
                Assert.NotNull(key);
                Assert.True(value.InEnqueuedState);
                Assert.NotNull(value.EnqueuedAt);
                Assert.NotNull(value.State);
                Assert.NotNull(value.Job);
            });
        }

        [Fact]
        public void FetchedCount_ReturnsActualValue()
        {
            // Arrange
            var backgroundJobClient = new BackgroundJobClient(_storage);
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-2"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-3"));
            var _ = _storage.GetConnection().FetchNextJob(new[] { "default" }, CancellationToken.None);

            // Act
            var fetchedCount = _monitoringApi.FetchedCount("default");

            // Assert
            Assert.Equal(1, fetchedCount);
        }

        [Fact]
        public void Queues_ReturnsActualQueues()
        {
            // Arrange
            var backgroundJobClient = new BackgroundJobClient(_storage);
            backgroundJobClient.Create(() => Worker.DoWork("hello-1"), new EnqueuedState("default"));
            backgroundJobClient.Create(() => Worker.DoWork("hello-2"), new EnqueuedState("test1"));
            backgroundJobClient.Create(() => Worker.DoWork("hello-3"), new EnqueuedState("test2"));
            backgroundJobClient.Create(() => Worker.DoWork("hello-4"), new EnqueuedState("default"));

            using var server = new BackgroundJobServer(_storage);
            // Act
            Thread.Sleep(5000); // -- wait till server completes boot

            var queues = _monitoringApi
                .Queues()
                .Select(x => x.Name)
                .ToArray();

            // Assert
            Assert.Contains("default", queues);
            Assert.Contains("test1", queues);
            Assert.Contains("test2", queues);
        }

        [Fact]
        public void JobDetails_ReturnsJobDetails()
        {
            // Arrange
            var backgroundJobClient = new BackgroundJobClient(_storage);
            var jobId = backgroundJobClient.Enqueue(() => Worker.DoWork("hello"));

            // Act
            var details = _monitoringApi.JobDetails(jobId);

            // Assert
            Assert.NotNull(details.Job);
            Assert.NotNull(details.CreatedAt);
            Assert.NotEmpty(details.History);
            Assert.NotEmpty(details.Properties);
        }

        [Fact]
        public void GetTimelineStats_ReturnsCounters()
        {
            // Arrange
            var backgroundJobClient = new BackgroundJobClient(_storage);

            var backgroundJobServer = new BackgroundJobServer(new BackgroundJobServerOptions
            {
                ServerCheckInterval = TimeSpan.FromSeconds(15),
                HeartbeatInterval = TimeSpan.FromSeconds(5),
                ServerTimeout = TimeSpan.FromSeconds(15),
                ServerName = "Hangfire Test Server",
                WorkerCount = 5,
                Queues = new[] { "queue2", "queue1", "default" }
            }, _storage);

            backgroundJobClient.Enqueue(() => Worker.DoWork("hello"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-2"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-3"));
            backgroundJobClient.Enqueue(() => Worker.Fail("hello-4"));
            backgroundJobClient.Enqueue(() => Worker.Fail("hello-5"));

            Thread.Sleep(TimeSpan.FromSeconds(10));

            backgroundJobServer.SendStop();
            backgroundJobServer.WaitForShutdown(TimeSpan.FromMinutes(1));
            backgroundJobServer.Dispose();

            // Act
            var succeededByDatesCount = _monitoringApi.SucceededByDatesCount();
            var failedByDatesCount = _monitoringApi.FailedByDatesCount();

            // Assert
            Assert.NotEmpty(succeededByDatesCount);
            Assert.Equal(3, succeededByDatesCount.First().Value);

            Assert.NotEmpty(failedByDatesCount);
            Assert.Equal(2, failedByDatesCount.First().Value);
        }

        [Fact]
        public void Servers_ReturnsActualServers()
        {
            // Arrange
            var queues = new[] { "default", "test" };
            var workerCount = 10;
            _storage.GetConnection().AnnounceServer("test-server", new ServerContext
            {
                Queues = queues,
                WorkerCount = workerCount
            });

            // Act
            var server = _monitoringApi
                .Servers()
                .Single();

            Assert.Equal("test-server", server.Name);
            Assert.Equal(queues, server.Queues);
            Assert.Equal(workerCount, server.WorkersCount);
        }

        [Fact]
        public void GetStatistics_ReturnsStatistics()
        {
            // Arrange
            var queues = new[] { "default", "test" };
            var workerCount = 10;
            _storage.GetConnection().AnnounceServer("test-server", new ServerContext
            {
                Queues = queues,
                WorkerCount = workerCount
            });

            var backgroundJobClient = new BackgroundJobClient(_storage);
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-2"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-3"));

            // Act
            var statistics = _monitoringApi.GetStatistics();

            // Assert
            Assert.NotEqual(0, statistics.Servers);
            Assert.NotEqual(0, statistics.Enqueued);
            Assert.NotEqual(0, statistics.Queues);
        }

        [SuppressMessage("ReSharper", "UnusedParameter.Global")]
        [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
        public static class Worker
        {
            public static void DoWork(string argument) { }

            [AutomaticRetry(Attempts = 0, OnAttemptsExceeded = AttemptsExceededAction.Fail)]
            public static void Fail(string argument) => throw new Exception("TEST OK!");
        }

        public MonitoringApiIntegrationTests(StorageContext<MonitoringApiIntegrationTests> storageContext, ITestOutputHelper testOutputHelper) : base(
            storageContext,
            testOutputHelper)
        {
            _storage = Storage;
            _monitoringApi = Storage.GetMonitoringApi();
        }
    }
}
