using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Entities;
using Hangfire.Storage;
using Npgsql;

// ReSharper disable RedundantAnonymousTypePropertyName
namespace Hangfire.PostgreSql
{
    internal class PostgreSqlJobQueue : IPersistentJobQueue
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
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            long timeoutSeconds = (long)_options.InvisibilityTimeout.Negate().TotalSeconds;

            var fetchJobSqlTemplate = $@"
UPDATE ""{_options.SchemaName}"".jobqueue AS jobqueue
SET fetchedat = NOW() AT TIME ZONE 'UTC'
WHERE jobqueue.id = (
    SELECT id
    FROM ""{_options.SchemaName}"".jobqueue
    WHERE queue IN ({string.Join(",", queues.Select(x => string.Concat("'", x, "'")))})
    AND (
       fetchedat IS NULL OR
       fetchedat < NOW() AT TIME ZONE 'UTC' + INTERVAL '{timeoutSeconds} SECONDS'
       )
    ORDER BY ({string.Join(",", queues.Select(x => $@"queue='{x}'"))}) DESC
    LIMIT 1
    FOR UPDATE SKIP LOCKED)
RETURNING jobqueue.id AS Id, jobid AS JobId, queue AS Queue, fetchedat AS FetchedAt;
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
                _options,
                fetchedJob.Id,
                fetchedJob.JobId.ToString(CultureInfo.InvariantCulture),
                fetchedJob.Queue);
        }

        public void Enqueue(string queue, string jobId)
        {
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                Enqueue(queue, jobId, connectionHolder.Connection);
            }
        }

        public void Enqueue(string queue, string jobId, NpgsqlConnection connection)
        {
            string query = $@"
INSERT INTO ""{_options.SchemaName}"".jobqueue (jobid, queue) 
VALUES (@jobId, @queue);
";
            var parameters = new { jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture), queue = queue };
            connection.Execute(query, parameters);
        }
    }
}
