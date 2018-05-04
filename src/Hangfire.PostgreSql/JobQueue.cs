using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Entities;
using Hangfire.Storage;

// ReSharper disable RedundantAnonymousTypePropertyName
namespace Hangfire.PostgreSql
{
    internal sealed class JobQueue : IJobQueue
    {
        private readonly IConnectionProvider _connectionProvider;
        private readonly PostgreSqlStorageOptions _options;

        public JobQueue(IConnectionProvider connectionProvider, PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(options, nameof(options));

            _connectionProvider = connectionProvider;
            _options = options;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            Guard.ThrowIfCollectionIsNullOrEmpty(queues, nameof(queues));

            var queuesOrder = string.Join(",", queues.Select(x => $@"queue='{x}'"));

            var fetchJobSqlTemplate = $@"
BEGIN;
UPDATE jobqueue AS jobqueue
SET fetchedat = NOW() AT TIME ZONE 'UTC'
WHERE jobqueue.id = (
    SELECT id
    FROM jobqueue
    WHERE queue IN @queues
    AND (
       fetchedat IS NULL OR
       fetchedat < NOW() AT TIME ZONE 'UTC' - @timeout
       )
    ORDER BY ({queuesOrder}) DESC
    LIMIT 1
    FOR UPDATE SKIP LOCKED)
RETURNING jobqueue.id AS Id, jobid AS JobId, queue AS Queue, fetchedat AS FetchedAt;
COMMIT;
";

            FetchedJob fetchedJob;
            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var connectionHolder = _connectionProvider.AcquireConnection())
                {
                    fetchedJob = connectionHolder.Connection.Query<FetchedJob>(
                            fetchJobSqlTemplate,
                            new { queues = queues, timeout = _options.InvisibilityTimeout })
                        .SingleOrDefault();
                }

                if (fetchedJob == null)
                {
                    cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                }

            } while (fetchedJob == null);

            return new Job(
                _connectionProvider,
                fetchedJob.Id,
                fetchedJob.JobId.ToString(CultureInfo.InvariantCulture),
                fetchedJob.Queue);
        }

        public void Enqueue(string queue, string jobId)
        {
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                const string query = @"
INSERT INTO jobqueue (jobid, queue) 
VALUES (@jobId, @queue)
";
                var parameters = new { jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture), queue = queue };
                connectionHolder.Connection.Execute(query, parameters);
            }
        }
    }
}
