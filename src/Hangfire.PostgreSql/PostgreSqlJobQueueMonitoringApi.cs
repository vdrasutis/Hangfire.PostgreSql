using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Dapper;

namespace Hangfire.PostgreSql
{
    internal class PostgreSqlJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly IDbConnection _connection;
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlJobQueueMonitoringApi(IDbConnection connection, PostgreSqlStorageOptions options)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public IEnumerable<string> GetQueues()
        {
            string sqlQuery = $@"
SELECT DISTINCT ""queue"" 
FROM ""{_options.SchemaName}"".""jobqueue"";
";
            return _connection.Query(sqlQuery).Select(x => (string) x.queue).ToList();
        }

        public IEnumerable<int> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            return GetQueuedOrFetchedJobIds(queue, false, @from, perPage);
        }

        private IEnumerable<int> GetQueuedOrFetchedJobIds(string queue, bool fetched, int @from, int perPage)
        {
            string sqlQuery = string.Format($@"
SELECT j.""id"" 
FROM ""{_options.SchemaName}"".""jobqueue"" jq
LEFT JOIN ""{_options.SchemaName}"".""job"" j ON jq.""jobid"" = j.""id""
WHERE jq.""queue"" = @queue 
AND jq.""fetchedat"" {{0}}
AND j.""id"" IS NOT NULL
LIMIT @count OFFSET @start;
", fetched ? "IS NOT NULL" : "IS NULL");

            return _connection.Query<int>(
                    sqlQuery,
                    new {queue = queue, start = @from, count = perPage})
                .ToList();
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            return GetQueuedOrFetchedJobIds(queue, true, @from, perPage);
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            string sqlQuery = @"
SELECT (
        SELECT COUNT(*) 
        FROM """ + _options.SchemaName + @""".""jobqueue"" 
        WHERE ""fetchedat"" IS NULL 
        AND ""queue"" = @queue
    ) ""EnqueuedCount"", 
    (
        SELECT COUNT(*) 
        FROM """ + _options.SchemaName + @""".""jobqueue"" 
        WHERE ""fetchedat"" IS NOT NULL 
        AND ""queue"" = @queue
    ) ""FetchedCount"";
";

            var result = _connection.Query(sqlQuery, new {queue = queue}).Single();

            return new EnqueuedAndFetchedCountDto
            {
                EnqueuedCount = result.EnqueuedCount,
                FetchedCount = result.FetchedCount
            };
        }
    }
}
