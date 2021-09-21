using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Locking;
using Hangfire.PostgreSql.Queueing;
using Hangfire.PostgreSql.Storage;
using Hangfire.PostgreSql.Tests.Setup;
using Hangfire.Server;
using Hangfire.Storage;
using Moq;
using Npgsql;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.PostgreSql.Tests.Integration
{
    public class StorageConnectionIntegrationTests : StorageContextBasedTests<StorageConnectionIntegrationTests>
    {
        private readonly PostgreSqlStorageOptions _options;
        private readonly IJobQueue _queue;
        private readonly IConnectionProvider _connectionProvider;
        private readonly ILockService _lockService;
        private readonly StorageConnection _storageConnection;

        public StorageConnectionIntegrationTests(StorageContext<StorageConnectionIntegrationTests> storageContext, ITestOutputHelper testOutputHelper)
            : base(storageContext, testOutputHelper)
        {
            _options = new PostgreSqlStorageOptions();
            _queue = new JobQueue(ConnectionProvider, _options);
            _connectionProvider = ConnectionProvider;
            _lockService = LockService;
            _storageConnection = new StorageConnection(_connectionProvider, _lockService, _queue, _options);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new StorageConnection(null, _lockService, _queue, _options));

            Assert.Equal("connectionProvider", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new StorageConnection(ConnectionProvider, _lockService, null, _options));

            Assert.Equal("queue", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new StorageConnection(ConnectionProvider, _lockService, _queue, null));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact]
        public void Dispose_DoesNotDisposeTheConnection()
        {
            var sqlConnection = ConnectionProvider;
            var connection = new StorageConnection(sqlConnection, _lockService, _queue, _options);

            connection.Dispose();

            // Assert.Equal(sqlConnection.); TODO
        }

        [Fact]
        public void FetchNextJob_DelegatesItsExecution_ToTheQueue()
        {
            // Arrange
            var queue = new Mock<IJobQueue>();
            var storageConnection = new StorageConnection(_connectionProvider, _lockService, queue.Object, _options);
            var token = new CancellationToken();
            var queues = new[] { "default" };

            // Act
            storageConnection.FetchNextJob(queues, token);

            // Assert
            queue.Verify(x => x.Dequeue(queues, token));
        }

        [Fact]
        public void CreateWriteTransaction_ReturnsNonNullInstance()
        {
            using (var transaction = _storageConnection.CreateWriteTransaction())
            {
                Assert.NotNull(transaction);
            }
        }

        [Fact]
        public void AcquireLock_ReturnsNonNullInstance()
        {
            UseConnection(connection =>
            {
                var @lock = connection.AcquireDistributedLock("1", TimeSpan.FromSeconds(1));
                Assert.NotNull(@lock);
            });
        }

        [Fact]
        public void CreateExpiredJob_ThrowsAnException_WhenJobIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.CreateExpiredJob(
                        null,
                        new Dictionary<string, string>(),
                        DateTime.UtcNow,
                        TimeSpan.Zero));

                Assert.Equal("job", exception.ParamName);
            });
        }

        [Fact]
        public void CreateExpiredJob_ThrowsAnException_WhenParametersCollectionIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.CreateExpiredJob(
                        Job.FromExpression(() => Worker.DoWork("hello")),
                        null,
                        DateTime.UtcNow,
                        TimeSpan.Zero));

                Assert.Equal("parameters", exception.ParamName);
            });
        }

        [Fact]
        public void CreateExpiredJob_CreatesAJobInTheStorage_AndSetsItsParameters()
        {
            UseConnections((sql, connection) =>
            {
                var createdAt = new DateTime(2012, 12, 12);
                var jobId = connection.CreateExpiredJob(
                    Job.FromExpression(() => Worker.DoWork("Hello")),
                    new Dictionary<string, string> { { "Key1", "Value1" }, { "Key2", "Value2" } },
                    createdAt,
                    TimeSpan.FromDays(1));

                Assert.NotNull(jobId);
                Assert.NotEmpty(jobId);

                var sqlJob = sql.Query(@"select * from job").Single();
                Assert.Equal(jobId, sqlJob.id.ToString());
                Assert.Equal(createdAt, sqlJob.createdat);
                Assert.Null((int?)sqlJob.stateid);
                Assert.Null((string)sqlJob.statename);

                var invocationData = SerializationHelper.Deserialize<InvocationData>((string)sqlJob.invocationdata);
                invocationData.Arguments = sqlJob.arguments;

                var job = invocationData.DeserializeJob();
                Assert.Equal(typeof(Worker), job.Type);
                Assert.Equal(nameof(Worker.DoWork), job.Method.Name);
                Assert.Equal("Hello", job.Args[0]);

                Assert.True(createdAt.AddDays(1).AddMinutes(-1) < sqlJob.expireat);
                Assert.True(sqlJob.expireat < createdAt.AddDays(1).AddMinutes(1));

                var parameters = sql.Query(
                        @"select * from jobparameter where jobid = @id",
                        new { id = JobId.ToLong(jobId) })
                    .ToDictionary(x => (string)x.name, x => (string)x.value);

                Assert.Equal("Value1", parameters["Key1"]);
                Assert.Equal("Value2", parameters["Key2"]);
            });
        }

        [Fact]
        public void GetJobData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>(
                () => connection.GetJobData(null)));
        }

        [Fact]
        public void GetJobData_ReturnsNull_WhenThereIsNoSuchJob()
        {
            UseConnection(connection =>
            {
                var result = connection.GetJobData("1");
                Assert.Null(result);
            });
        }

        [Fact]
        public void GetJobData_ReturnsResult_WhenJobExists()
        {
            string arrangeSql = @"
insert into job (invocationdata, arguments, statename, createdat)
values (@invocationData, @arguments, @stateName, now() at time zone 'utc') returning id";

            UseConnections((sql, connection) =>
            {
                var job = Job.FromExpression(() => Worker.DoWork("wrong"));

                var jobId = (int)sql.Query(
                    arrangeSql,
                    new
                    {
                        invocationData = SerializationHelper.Serialize(InvocationData.SerializeJob(job)),
                        stateName = "Succeeded",
                        arguments = "[\"\\\"Arguments\\\"\"]"
                    }).Single().id;

                var result = connection.GetJobData(jobId.ToString());
                var now = DateTime.UtcNow.AddMinutes(-1);

                Assert.NotNull(result);
                Assert.NotNull(result.Job);
                Assert.Equal("Succeeded", result.State);
                Assert.Equal("Arguments", result.Job.Args[0]);
                Assert.Null(result.LoadException);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
                Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
            });
        }

        [Fact]
        public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(
                connection => Assert.Throws<ArgumentNullException>(
                    () => connection.GetStateData(null)));
        }

        [Fact]
        public void GetStateData_ReturnsNull_IfThereIsNoSuchState()
        {
            UseConnection(connection =>
            {
                var result = connection.GetStateData("1");
                Assert.Null(result);
            });
        }

        [Fact]
        public void GetStateData_ReturnsCorrectData()
        {
            string createJobSql = @"
insert into job (invocationdata, arguments, statename, createdat)
    values ('', '', '', now() at time zone 'utc') returning id;
            ";

            string createStateSql = @"
insert into state (jobid, name, createdat)
values(@jobId, 'old-state', now() at time zone 'utc');

insert into state (jobid, name, reason, data, createdat)
values(@jobId, @name, @reason, @data, now() at time zone 'utc')
returning id;";

            string updateJobStateSql = @"
    update job
    set stateid = @stateId
    where id = @jobId;
";

            UseConnections((sql, connection) =>
            {
                var data = new Dictionary<string, string>
                {
                    { "Key", "Value" }
                };

                var jobId = (int)sql.Query(createJobSql).Single().id;

                var stateId = (int)sql.Query(
                        createStateSql,
                        new { jobId = jobId, name = "Name", reason = "Reason", @data = SerializationHelper.Serialize(data) })
                    .Single()
                    .id;

                sql.Execute(updateJobStateSql, new { jobId = jobId, stateId = stateId });

                var result = connection.GetStateData(jobId.ToString(CultureInfo.InvariantCulture));
                Assert.NotNull(result);

                Assert.Equal("Name", result.Name);
                Assert.Equal("Reason", result.Reason);
                Assert.Equal("Value", result.Data["Key"]);
            });
        }

        [Fact]
        public void GetJobData_ReturnsJobLoadException_IfThereWasADeserializationException()
        {
            string arrangeSql = @"
insert into job (invocationdata, arguments, statename, createdat)
values (@invocationData, @arguments, @stateName, now() at time zone 'utc') returning id";

            UseConnections((sql, connection) =>
            {
                var jobId = sql.Query(
                    arrangeSql,
                    new
                    {
                        invocationData = SerializationHelper.Serialize(new InvocationData(null, null, null, null)),
                        stateName = "Succeeded",
                        arguments = "['Arguments']"
                    }).Single();

                var result = connection.GetJobData(((int)jobId.id).ToString());

                Assert.NotNull(result.LoadException);
            });
        }

        [Fact]
        public void SetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter(null, "name", "value"));

                Assert.Equal("id", exception.ParamName);
            });
        }

        [Fact]
        public void SetParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter("1", null, "value"));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact]
        public void SetParameters_CreatesNewParameter_WhenParameterWithTheGivenNameDoesNotExists()
        {
            string arrangeSql = @"
insert into job (invocationdata, arguments, createdat)
values ('', '', now() at time zone 'utc') returning id";

            UseConnections((sql, connection) =>
            {
                var job = sql.Query(arrangeSql).Single();
                string jobId = job.id.ToString();

                connection.SetJobParameter(jobId, "Name", "Value");

                var parameter = sql.Query(
                    @"select * from jobparameter where jobid = @id and name = @name",
                    new { id = JobId.ToLong(jobId), name = "Name" }).Single();

                Assert.Equal("Value", parameter.value);
            });
        }

        [Fact]
        public void SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
        {
            const string arrangeSql = @"
insert into job (invocationdata, arguments, createdat)
values ('', '', now() at time zone 'utc') returning id";

            UseConnections((sql, connection) =>
            {
                var job = sql.Query(arrangeSql).Single();
                string jobId = job.id.ToString();

                connection.SetJobParameter(jobId, "Name", "Value");
                connection.SetJobParameter(jobId, "Name", "AnotherValue");

                var parameter = sql.Query(
                    @"select * from jobparameter where jobid = @id and name = @name",
                    new { id = JobId.ToLong(jobId), name = "Name" }).Single();

                Assert.Equal("AnotherValue", parameter.value);
            });
        }

        [Fact]
        public void SetParameter_CanAcceptNulls_AsValues()
        {
            const string arrangeSql = @"
insert into job (invocationdata, arguments, createdat)
values ('', '', now() at time zone 'utc') returning id";

            UseConnections((sql, connection) =>
            {
                var job = sql.Query(arrangeSql).Single();
                string jobId = job.id.ToString();

                connection.SetJobParameter(jobId, "Name", null);

                var parameter = sql.Query(
                    @"select * from jobparameter where jobid = @id and name = @name",
                    new { id = JobId.ToLong(jobId), name = "Name" }).Single();

                Assert.Equal((string)null!, parameter.value);
            });
        }

        [Fact]
        public void GetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobParameter(null, "hello"));

                Assert.Equal("id", exception.ParamName);
            });
        }

        [Fact]
        public void GetParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobParameter("1", null));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact]
        public void GetParameter_ReturnsNull_WhenParameterDoesNotExists()
        {
            UseConnection(connection =>
            {
                var value = connection.GetJobParameter("1", "hello");
                Assert.Null(value);
            });
        }

        [Fact]
        public void GetParameter_ReturnsParameterValue_WhenJobExists()
        {
            const string arrangeSql = @"
with insertedjob as (
    insert into job (invocationdata, arguments, createdat)
    values ('', '', now() at time zone 'utc') returning id
)
insert into jobparameter (jobid, name, value)
select insertedjob.id, @name, @value
from insertedjob
returning jobid;
";
            UseConnections((sql, connection) =>
            {
                var id = sql.Query<int>(
                    arrangeSql,
                    new { name = "name", value = "value" }).Single();

                var value = connection.GetJobParameter(JobId.ToString(id), "name");

                Assert.Equal("value", value);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetFirstByLowestScoreFromSet(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_ToScoreIsLowerThanFromScore()
        {
            UseConnection(connection => Assert.Throws<ArgumentException>(
                () => connection.GetFirstByLowestScoreFromSet("key", 0, -1)));
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenTheKeyDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetFirstByLowestScoreFromSet(
                    "key", 0, 1);

                Assert.Null(result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore()
        {
            const string arrangeSql = @"
insert into set (key, score, value)
values 
('key', 1.0, '1.0'),
('key', -1.0, '-1.0'),
('key', -5.0, '-5.0'),
('another-key', -2.0, '-2.0')";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql);

                var result = connection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0);

                Assert.Equal("-1.0", result);
            });
        }

        [Fact]
        public void AnnounceServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.AnnounceServer(null, new ServerContext()));

                Assert.Equal("serverId", exception.ParamName);
            });
        }

        [Fact]
        public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.AnnounceServer("server", null));

                Assert.Equal("context", exception.ParamName);
            });
        }

        [Fact]
        public void AnnounceServer_CreatesOrUpdatesARecord()
        {
            UseConnections((sql, connection) =>
            {
                var context1 = new ServerContext
                {
                    Queues = new[] { "critical", "default" },
                    WorkerCount = 4
                };
                connection.AnnounceServer("server", context1);

                var server = sql.Query(@"select * from server").Single();
                Assert.Equal("server", server.id);
                Assert.True(((string)server.data).StartsWith(
                        "{\"WorkerCount\":4,\"Queues\":[\"critical\",\"default\"],\"StartedAt\":"),
                    server.data);
                Assert.NotNull(server.lastheartbeat);

                var context2 = new ServerContext
                {
                    Queues = new[] { "default" },
                    WorkerCount = 1000
                };
                connection.AnnounceServer("server", context2);
                var sameServer = sql.Query(@"select * from server").Single();
                Assert.Equal("server", sameServer.id);
                Assert.Contains("1000", sameServer.data);
            });
        }

        [Fact]
        public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>(
                () => connection.RemoveServer(null)));
        }

        [Fact]
        public void RemoveServer_RemovesAServerRecord()
        {
            const string arrangeSql = @"
insert into server (id, data, lastheartbeat)
values ('Server1', '', now() at time zone 'utc'),
('Server2', '', now() at time zone 'utc')";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql);

                connection.RemoveServer("Server1");

                var server = sql.Query(@"select * from server").Single();
                Assert.NotEqual("Server1", server.Id, StringComparer.OrdinalIgnoreCase);
            });
        }

        [Fact]
        public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>(
                () => connection.Heartbeat(null)));
        }

        [Fact]
        public void Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenId()
        {
            const string arrangeSql = @"
insert into server (id, data, lastheartbeat)
values
('server1', '', '2012-12-12 12:12:12'),
('server2', '', '2012-12-12 12:12:12')";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql);

                connection.Heartbeat("server1");

                var servers = sql.Query(@"select * from server")
                    .ToDictionary(x => (string)x.id, x => (DateTime)x.lastheartbeat);

                Assert.NotEqual(2012, servers["server1"].Year);
                Assert.Equal(2012, servers["server2"].Year);
            });
        }

        [Fact]
        public void RemoveTimedOutServers_ThrowsAnException_WhenTimeOutIsNegative()
        {
            UseConnection(connection => Assert.Throws<ArgumentException>(
                () => connection.RemoveTimedOutServers(TimeSpan.FromMinutes(-5))));
        }

        [Fact]
        public void RemoveTimedOutServers_DoItsWorkPerfectly()
        {
            const string arrangeSql = @"
insert into server (id, data, lastheartbeat)
values (@id, '', @heartbeat)";

            UseConnections((sql, connection) =>
            {
                sql.Execute(
                    arrangeSql,
                    new[]
                    {
                        new { id = "server1", heartbeat = DateTime.UtcNow.AddDays(-1) },
                        new { id = "server2", heartbeat = DateTime.UtcNow.AddHours(-12) }
                    });

                connection.RemoveTimedOutServers(TimeSpan.FromHours(15));

                var liveServer = sql.Query(@"select * from server").Single();
                Assert.Equal("server2", liveServer.id);
            });
        }

        [Fact]
        public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
                Assert.Throws<ArgumentNullException>(() => connection.GetAllItemsFromSet(null)));
        }

        [Fact]
        public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenKeyDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllItemsFromSet("some-set");

                Assert.NotNull(result);
                Assert.Empty(result);
            });
        }

        [Fact]
        public void GetAllItemsFromSet_ReturnsAllItems()
        {
            const string arrangeSql = @"
insert into set (key, score, value)
values (@key, 0.0, @value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "some-set", value = "1" },
                    new { key = "some-set", value = "2" },
                    new { key = "another-set", value = "3" }
                });

                // Act
                var result = connection.GetAllItemsFromSet("some-set");

                // Assert
                Assert.Equal(2, result.Count);
                Assert.Contains("1", result);
                Assert.Contains("2", result);
            });
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash(null, new Dictionary<string, string>()));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash("some-hash", null));

                Assert.Equal("keyValuePairs", exception.ParamName);
            });
        }

        [Fact]
        public void SetRangeInHash_MergesAllRecords()
        {
            UseConnections((sql, connection) =>
            {
                connection.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                });

                var result = sql.Query(
                        @"select * from hash where key = @key",
                        new { key = "some-hash" })
                    .ToDictionary(x => (string)x.field, x => (string)x.value);

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact]
        public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
                Assert.Throws<ArgumentNullException>(() => connection.GetAllEntriesFromHash(null)));
        }

        [Fact]
        public void GetAllEntriesFromHash_ReturnsNull_IfHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllEntriesFromHash("some-hash");
                Assert.Null(result);
            });
        }

        [Fact]
        public void GetAllEntriesFromHash_ReturnsAllKeysAndTheirValues()
        {
            string arrangeSql = @"
insert into hash (key, field, value)
values (@key, @field, @value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "some-hash", field = "Key1", value = "Value1" },
                    new { key = "some-hash", field = "Key2", value = "Value2" },
                    new { key = "another-hash", field = "Key3", value = "Value3" }
                });

                // Act
                var result = connection.GetAllEntriesFromHash("some-hash");

                // Assert
                Assert.NotNull(result);
                Assert.Equal(2, result.Count);
                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact]
        public void GetSetCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetSetCount(null));
            });
        }

        [Fact]
        public void GetSetCount_ReturnsZero_WhenSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetSetCount("my-set");
                Assert.Equal(0, result);
            });
        }

        [Fact]
        public void GetSetCount_ReturnsNumberOfElements_InASet()
        {
            const string arrangeSql = @"insert into set (key, value, score) values (@key, @value, 0.0)";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql, new List<dynamic>
                {
                    new { key = "set-1", value = "value-1" },
                    new { key = "set-2", value = "value-1" },
                    new { key = "set-1", value = "value-2" }
                });

                var result = connection.GetSetCount("set-1");

                Assert.Equal(2, result);
            });
        }

        [Fact]
        public void GetAllItemsFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetAllItemsFromList(null));
            });
        }

        [Fact]
        public void GetAllItemsFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllItemsFromList("my-list");
                Assert.Empty(result);
            });
        }

        [Fact]
        public void GetAllItemsFromList_ReturnsAllItems_FromAGivenList()
        {
            const string arrangeSql = @"insert into list (key, value) values (@key, @value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "list-1", value = "1" },
                    new { key = "list-2", value = "2" },
                    new { key = "list-1", value = "3" }
                });

                // Act
                var result = connection.GetAllItemsFromList("list-1");

                // Assert
                Assert.Equal(new[] { "3", "1" }, result);
            });
        }

        [Fact]
        public void GetCounter_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetCounter(null));
            });
        }

        [Fact]
        public void GetCounter_ReturnsZero_WhenKeyDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetCounter("my-counter");
                Assert.Equal(0, result);
            });
        }

        [Fact]
        public void GetCounter_ReturnsSumOfValues_InCounterTable()
        {
            const string arrangeSql = @"insert into counter (key, value) values (@key, @value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "counter-1", value = 1 },
                    new { key = "counter-2", value = 1 },
                    new { key = "counter-1", value = 1 }
                });

                // Act
                var result = connection.GetCounter("counter-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact]
        public void GetListCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetListCount(null!));
            });
        }

        [Fact]
        public void GetListCount_ReturnsZero_WhenListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetListCount("my-list");
                Assert.Equal(0, result);
            });
        }

        [Fact]
        public void GetListCount_ReturnsTheNumberOfListElements()
        {
            const string arrangeSql = @"insert into list (key) values (@key)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "list-1" },
                    new { key = "list-1" },
                    new { key = "list-2" }
                });

                // Act
                var result = connection.GetListCount("list-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact]
        public void GetListTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetListTtl(null!));
            });
        }

        [Fact]
        public void GetListTtl_ReturnsNegativeValue_WhenListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetListTtl("my-list");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact]
        public void GetListTtl_ReturnsExpirationTimeForList()
        {
            const string arrangeSql = @"insert into list (key, expireat) values (@key, @expireAt)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "list-1", expireAt = (DateTime?)DateTime.UtcNow.AddHours(1) },
                    new { key = "list-2", expireAt = (DateTime?)null }
                });

                // Act
                var result = connection.GetListTtl("list-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact]
        public void GetRangeFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetRangeFromList(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetRangeFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetRangeFromList("my-list", 0, 1);
                Assert.Empty(result);
            });
        }

        [Fact]
        public void GetRangeFromList_ReturnsAllEntries_WithinGivenBounds()
        {
            const string arrangeSql = @"insert into list (key, value) values (@key, @value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "list-1", value = "1" },
                    new { key = "list-2", value = "2" },
                    new { key = "list-1", value = "3" },
                    new { key = "list-1", value = "4" },
                    new { key = "list-1", value = "5" }
                });

                // Act
                var result = connection.GetRangeFromList("list-1", 1, 2);

                // Assert
                Assert.Equal(new[] { "4", "3" }, result);
            });
        }

        [Fact]
        public void GetHashCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => connection.GetHashCount(null)); });
        }

        [Fact]
        public void GetHashCount_ReturnsZero_WhenKeyDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetHashCount("my-hash");
                Assert.Equal(0, result);
            });
        }

        [Fact]
        public void GetHashCount_ReturnsNumber_OfHashFields()
        {
            const string arrangeSql = @"insert into hash (key, field) values (@key, @field)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "hash-1", field = "field-1" },
                    new { key = "hash-1", field = "field-2" },
                    new { key = "hash-2", field = "field-1" }
                });

                // Act
                var result = connection.GetHashCount("hash-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact]
        public void GetHashTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetHashTtl(null));
            });
        }

        [Fact]
        public void GetHashTtl_ReturnsNegativeValue_WhenHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetHashTtl("my-hash");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact]
        public void GetHashTtl_ReturnsExpirationTimeForHash()
        {
            const string arrangeSql = @"insert into hash (key, field, expireat) values (@key, @field, @expireAt)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "hash-1", field = "field", expireAt = (DateTime?)DateTime.UtcNow.AddHours(1) },
                    new { key = "hash-2", field = "field", expireAt = (DateTime?)null }
                });

                // Act
                var result = connection.GetHashTtl("hash-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact]
        public void GetRangeFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => connection.GetRangeFromSet(null, 0, 1)); });
        }

        [Fact]
        public void GetRangeFromSet_ReturnsPagedElements()
        {
            const string arrangeSql = @"insert into set (key, value, score) values (@key, @value, 0.0)";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql, new List<dynamic>
                {
                    new { key = "set-1", value = "1" },
                    new { key = "set-1", value = "2" },
                    new { key = "set-1", value = "3" },
                    new { key = "set-1", value = "4" },
                    new { key = "set-2", value = "4" },
                    new { key = "set-1", value = "5" }
                });

                var result = connection.GetRangeFromSet("set-1", 2, 3);

                Assert.Equal(new[] { "3", "4" }, result);
            });
        }

        [Fact]
        public void GetSetTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => connection.GetSetTtl(null)); });
        }

        [Fact]
        public void GetSetTtl_ReturnsNegativeValue_WhenSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetSetTtl("my-set");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact]
        public void GetSetTtl_ReturnsExpirationTime_OfAGivenSet()
        {
            const string arrangeSql = @"insert into set (key, value, expireat, score) values (@key, @value, @expireAt, 0.0)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "set-1", value = "1", expireAt = (DateTime?)DateTime.UtcNow.AddMinutes(60) },
                    new { key = "set-2", value = "2", expireAt = (DateTime?)null }
                });

                // Act
                var result = connection.GetSetTtl("set-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact]
        public void GetValueFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetValueFromHash(null, "name"));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetValueFromHash_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetValueFromHash("key", null));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact]
        public void GetValueFromHash_ReturnsNull_WhenHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetValueFromHash("my-hash", "name");
                Assert.Null(result);
            });
        }

        [Fact]
        public void GetValueFromHash_ReturnsValue_OfAGivenField()
        {
            const string arrangeSql = @"insert into hash (key, field, value) values (@key, @field, @value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "hash-1", field = "field-1", value = "1" },
                    new { key = "hash-1", field = "field-2", value = "2" },
                    new { key = "hash-2", field = "field-1", value = "3" }
                });

                // Act
                var result = connection.GetValueFromHash("hash-1", "field-1");

                // Assert
                Assert.Equal("1", result);
            });
        }

        private void UseConnections(Action<NpgsqlConnection, StorageConnection> action)
        {
            var provider = ConnectionProvider;
            using var connection = new StorageConnection(provider, _lockService, _queue, _options);
            using var con = provider.AcquireConnection();
            action(con.Connection, connection);
        }

        private void UseConnection(Action<StorageConnection> action)
        {
            using var connection = new StorageConnection(
                ConnectionProvider,
                _lockService,
                _queue,
                _options);
            action(connection);
        }

        [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
        [SuppressMessage("ReSharper", "UnusedParameter.Global")]
        public static class Worker
        {
            public static void DoWork(string argument) { }
        }
    }
}
