using System;
using System.Threading;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Maintenance;
using Hangfire.PostgreSql.Tests.Setup;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.PostgreSql.Tests.Integration
{
    public class ExpirationManagerIntegrationTests : StorageContextBasedTests<ExpirationManagerIntegrationTests>
    {
        private readonly ExpirationManager _expirationManager;
        private readonly CancellationTokenSource _tokenSource;

        public ExpirationManagerIntegrationTests(StorageContext<ExpirationManagerIntegrationTests> storageContext, ITestOutputHelper testOutputHelper) : base(storageContext,
            testOutputHelper)
        {
            _expirationManager = new ExpirationManager(ConnectionProvider, TimeSpan.FromMilliseconds(1));
            _tokenSource = new CancellationTokenSource();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionProviderIsNull()
        {
            Assert.Throws<ArgumentNullException>(
                () => new ExpirationManager(null));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenCheckIntervalIsZero()
        {
            Assert.Throws<ArgumentException>(
                () => new ExpirationManager(ConnectionProvider, TimeSpan.Zero));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenCheckIntervalIsNegative()
        {
            Assert.Throws<ArgumentException>(
                () => new ExpirationManager(ConnectionProvider, TimeSpan.FromSeconds(-1)));
        }

        [Fact]
        public void Execute_RemovesOutdatedRecords()
        {
            // Arrange
            CreateCounterExpirationEntry(DateTime.UtcNow.AddDays(-1));

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = ConnectionProvider.FetchScalar<int>("select count(*) from counter");
            Assert.Equal(0, recordsCount);
        }

        [Fact]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            // Arrange
            CreateCounterExpirationEntry(null);

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = ConnectionProvider.FetchScalar<int>("select count(*) from counter");
            Assert.Equal(1, recordsCount);
        }

        [Fact]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            // Arrange
            CreateCounterExpirationEntry(DateTime.UtcNow.AddDays(1));

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = ConnectionProvider.FetchScalar<int>("select count(*) from counter");
            Assert.Equal(1, recordsCount);
        }

        [Fact]
        public void Execute_Processes_CounterTable()
        {
            // Arrange
            CreateCounterExpirationEntry(DateTime.UtcNow.AddDays(-1));

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = ConnectionProvider.FetchScalar<int>("select count(*) from counter");
            Assert.Equal(0, recordsCount);
        }

        private void CreateCounterExpirationEntry(DateTime? expireAt)
        {
            const string query = @"
insert into counter(key, value, expireat)
values ('key', 1, @expireat)";

            ConnectionProvider.Execute(query, new { expireat = expireAt });
        }

        [Fact]
        public void Execute_Processes_JobTable()
        {
            // Arrange
            const string query = @"
insert into job (invocationdata, arguments, createdat, expireat) 
values ('', '', now() at time zone 'utc', @expireAt)";
            ConnectionProvider.Execute(query, new { expireAt = DateTime.UtcNow.AddDays(-1) });

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = ConnectionProvider.FetchScalar<int>("select count(*) from job");
            Assert.Equal(0, recordsCount);
        }

        [Fact]
        public void Execute_Processes_ListTable()
        {
            // Arrange
            const string query = @"
insert into list (key, expireat) 
values ('key', @expireAt)";
            ConnectionProvider.Execute(query, new { expireAt = DateTime.UtcNow.AddDays(-1) });

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = ConnectionProvider.FetchScalar<int>("select count(*) from list");
            Assert.Equal(0, recordsCount);
        }

        [Fact]
        public void Execute_Processes_SetTable()
        {
            // Arrange
            const string query = @"
insert into set (key, score, value, expireat) 
values ('key', 0, '', @expireAt)";
            ConnectionProvider.Execute(query, new { expireAt = DateTime.UtcNow.AddDays(-1) });

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = ConnectionProvider.FetchScalar<int>("select count(*) from set");
            Assert.Equal(0, recordsCount);
        }

        [Fact]
        public void Execute_Processes_HashTable()
        {
            // Arrange
            const string query = @"
insert into hash (key, field, value, expireat) 
values ('key', 'field', '', @expireAt)";
            ConnectionProvider.Execute(query, new { expireAt = DateTime.UtcNow.AddDays(-1) });

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = ConnectionProvider.FetchScalar<int>("select count(*) from set");
            Assert.Equal(0, recordsCount);
        }
    }
}
