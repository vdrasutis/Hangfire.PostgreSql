using System.Collections.Generic;
using System.Linq;
using Dapper;

// ReSharper disable RedundantAnonymousTypePropertyName
namespace Hangfire.PostgreSql
{
    internal class PostgreSqlJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlJobQueueMonitoringApi(IPostgreSqlConnectionProvider connectionProvider, PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(options, nameof(options));

            _connectionProvider = connectionProvider;
            _options = options;
        }

        public IEnumerable<string> GetQueues()
        {
            string sqlQuery = $@"
SELECT DISTINCT ""queue"" 
FROM ""{_options.SchemaName}"".""jobqueue"";
";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query(sqlQuery).Select(x => (string)x.queue).ToList();
            }
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

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<int>(
                        sqlQuery,
                        new { queue = queue, start = @from, count = perPage })
                    .ToList();
            }
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
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var result = connectionHolder.Connection.Query(sqlQuery, new { queue = queue }).Single();

                return new EnqueuedAndFetchedCountDto
                {
                    EnqueuedCount = result.EnqueuedCount,
                    FetchedCount = result.FetchedCount
                };
            }
        }
    }
}
