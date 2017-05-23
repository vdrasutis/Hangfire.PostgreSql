using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Properties;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    public class PostgreSqlJobQueue : IPersistentJobQueue
    {
        private readonly PostgreSqlStorageOptions _options;
        private readonly IDbConnection _connection;

        public PostgreSqlJobQueue(IDbConnection connection, PostgreSqlStorageOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            long timeoutSeconds = (long) _options.InvisibilityTimeout.Negate().TotalSeconds;
            FetchedJob fetchedJob;

            var fetchJobSqlTemplate = $@"
WITH fetched AS (
   SELECT id
   FROM ""{_options.SchemaName}"".jobqueue
   WHERE queue = ANY (@queues)
   LIMIT 1
   FOR UPDATE SKIP LOCKED
)
UPDATE ""{_options.SchemaName}"".jobqueue AS jobqueue
SET fetchedat = NOW() AT TIME ZONE 'UTC'
FROM fetched
WHERE jobqueue.id = fetched.id
AND fetchedat {{0}}
RETURNING jobqueue.id AS Id, jobid AS JobId, queue AS Queue, fetchedat AS FetchedAt;
";

            var fetchConditions = new[]
                {"IS NULL", $"< NOW() AT TIME ZONE 'UTC' + INTERVAL '{timeoutSeconds} SECONDS'"};
            var currentQueryIndex = 0;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                string fetchJobSql = string.Format(fetchJobSqlTemplate, fetchConditions[currentQueryIndex]);

                Utils.Utils.TryExecute(() =>
                    {
                        var jobToFetch = _connection.Query<FetchedJob>(
                                fetchJobSql,
                                new {queues = queues.ToList()})
                            .SingleOrDefault();

                        return jobToFetch;
                    },
                    out fetchedJob,
                    ex =>
                    {
                        var smoothException = ex is PostgresException postgresException &&
                                              postgresException.SqlState.Equals("40001");
                        return smoothException;
                    });

                if (fetchedJob == null)
                {
                    if (currentQueryIndex == fetchConditions.Length - 1)
                    {
                        cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }

                currentQueryIndex = (currentQueryIndex + 1) % fetchConditions.Length;
            } while (fetchedJob == null);

            return new PostgreSqlFetchedJob(
                _connection,
                _options,
                fetchedJob.Id,
                fetchedJob.JobId.ToString(CultureInfo.InvariantCulture),
                fetchedJob.Queue);
        }

        public void Enqueue(string queue, string jobId)
        {
            string enqueueJobSql = $@"
INSERT INTO ""{_options.SchemaName}"".""jobqueue"" (""jobid"", ""queue"") 
VALUES (@jobId, @queue);
";

            _connection.Execute(enqueueJobSql,
                new {jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture), queue = queue});
        }

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        private class FetchedJob
        {
            public int Id { get; set; }
            public int JobId { get; set; }
            public string Queue { get; set; }
            public DateTime? FetchedAt { get; set; }
            public int UpdateCount { get; set; }
        }
    }
}
