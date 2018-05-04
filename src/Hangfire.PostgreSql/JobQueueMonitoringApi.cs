using System.Collections.Generic;
using System.Linq;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Entities;
using Hangfire.States;
using Hangfire.Storage.Monitoring;

// ReSharper disable RedundantAnonymousTypePropertyName
namespace Hangfire.PostgreSql
{
    internal class JobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private const string EnqueuedFetchCondition = "IS NULL";
        private const string FetchedFetchCondition = "IS NOT NULL";

        private readonly IConnectionProvider _connectionProvider;
        private readonly PostgreSqlStorageOptions _options;

        public JobQueueMonitoringApi(IConnectionProvider connectionProvider, PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(options, nameof(options));

            _connectionProvider = connectionProvider;
            _options = options;
        }

        public IEnumerable<string> GetQueues()
        {
            const string query = @"
SELECT DISTINCT queue 
FROM jobqueue;
";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query(query).Select(x => (string)x.queue).ToList();
            }
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            var enqueuedJobsQuery = GetQuery(queue, @from, perPage, EnqueuedState.StateName, EnqueuedFetchCondition);

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var jobs = connectionHolder.Connection.Query<SqlJob>(enqueuedJobsQuery).ToList();

                return Utils.DeserializeJobs(
                    jobs,
                    (sqlJob, job, stateData) => new EnqueuedJobDto
                    {
                        Job = job,
                        State = sqlJob.StateName,
                        EnqueuedAt = sqlJob.StateName == EnqueuedState.StateName
                            ? JobHelper.DeserializeNullableDateTime(stateData["EnqueuedAt"])
                            : null
                    });
            }
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            var fetchedJobsQuery = GetQuery(queue, @from, perPage, ProcessingState.StateName, FetchedFetchCondition);

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var jobs = connectionHolder.Connection.Query<SqlJob>(fetchedJobsQuery).ToList();

                return Utils.DeserializeJobs(
                    jobs,
                    (sqlJob, job, stateData) => new FetchedJobDto
                    {
                        Job = Utils.DeserializeJob(sqlJob.InvocationData, sqlJob.Arguments),
                        State = sqlJob.StateName,
                        FetchedAt = sqlJob.FetchedAt
                    });
            }
        }

        private static string GetQuery(string queue, int @from, int perPage, string stateName, string fetchCondition) => $@"
SELECT j.id ""Id"",
       j.invocationdata ""InvocationData"", 
       j.arguments ""Arguments"", 
       j.createdat ""CreatedAt"", 
       j.expireat ""ExpireAt"", 
       s.name ""StateName"", 
       s.reason""StateReason"", 
       s.data ""StateData""
FROM jobqueue jq
LEFT JOIN job j ON jq.jobid = j.id
LEFT JOIN state s ON s.id = j.stateid
WHERE jq.queue = '{queue}'
AND jq.fetchedat {fetchCondition}
AND s.name = '{stateName}'
LIMIT {perPage} OFFSET {from};";

        public (long? enqueued, long? fetched) GetEnqueuedAndFetchedCount(string queue)
        {
            const string query = @"
SELECT (
        SELECT COUNT(*) 
        FROM ""jobqueue"" 
        WHERE ""fetchedat"" IS NULL 
        AND ""queue"" = @queue
    ) ""EnqueuedCount"", 
    (
        SELECT COUNT(*) 
        FROM ""jobqueue"" 
        WHERE ""fetchedat"" IS NOT NULL 
        AND ""queue"" = @queue
    ) ""FetchedCount"";
";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var result = connectionHolder.Connection.Query(query, new { queue = queue }).Single();

                return (result.EnqueuedCount, result.FetchedCount);
            }
        }
    }
}
