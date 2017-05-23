using Dapper;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
    internal class PostgreSqlFetchedJob : IFetchedJob
    {
        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private readonly PostgreSqlStorageOptions _options;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;

        public PostgreSqlFetchedJob(
            IPostgreSqlConnectionProvider connectionProvider,
            PostgreSqlStorageOptions options,
            int id,
            string jobId,
            string queue)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(options, nameof(options));
            Guard.ThrowIfNull(jobId, nameof(jobId));
            Guard.ThrowIfNull(queue, nameof(queue));

            _connectionProvider = connectionProvider;
            _options = options;
            Id = id;
            JobId = jobId;
            Queue = queue;
        }

        public int Id { get; }
        public string JobId { get; }
        public string Queue { get; }

        public void RemoveFromQueue()
        {
            var sql = $@"
DELETE FROM ""{_options.SchemaName}"".jobqueue
WHERE id = @id;
";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                connectionHolder.Connection.Execute(sql, new { id = Id });
                _removedFromQueue = true;
            }
        }

        public void Requeue()
        {
            var sql = $@"
UPDATE ""{_options.SchemaName}"".""jobqueue"" 
SET ""fetchedat"" = NULL 
WHERE ""id"" = @id;
";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                connectionHolder.Connection.Execute(sql, new { id = Id });
                _requeued = true;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _disposed = true;
        }
    }
}
