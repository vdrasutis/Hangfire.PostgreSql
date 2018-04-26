using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Entities;
using Hangfire.Storage;

// ReSharper disable RedundantAnonymousTypePropertyName
namespace Hangfire.PostgreSql
{
    internal sealed class PostgreSqlJobQueue : IPersistentJobQueue
    {
        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlJobQueue(IPostgreSqlConnectionProvider connectionProvider, PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(options, nameof(options));

            _connectionProvider = connectionProvider;
            _options = options;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            Guard.ThrowIfCollectionIsNullOrEmpty(queues, nameof(queues));

            long timeoutSeconds = (long)_options.InvisibilityTimeout.Negate().TotalSeconds;

            var queuesList = string.Join(",", queues.Select(x => string.Concat("'", x, "'")));
            var queuesOrder = string.Join(",", queues.Select(x => $@"queue='{x}'"));

            var fetchJobSqlTemplate = $@"
BEGIN;
UPDATE jobqueue AS jobqueue
SET fetchedat = NOW() AT TIME ZONE 'UTC'
WHERE jobqueue.id = (
    SELECT id
    FROM jobqueue
    WHERE queue IN ({queuesList})
    AND (
       fetchedat IS NULL OR
       fetchedat < NOW() AT TIME ZONE 'UTC' + INTERVAL '{timeoutSeconds} SECONDS'
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
                            fetchJobSqlTemplate)
                        .SingleOrDefault();
                }

                if (fetchedJob == null)
                {
                    cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                }

            } while (fetchedJob == null);

            return new PostgreSqlFetchedJob(
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
VALUES (@jobId, @queue);
";
                var parameters = new { jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture), queue = queue };
                connectionHolder.Connection.Execute(query, parameters);
            }
        }
    }
}
