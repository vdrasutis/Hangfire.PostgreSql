using System;
using System.Data;
using Dapper;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
#if (NETCORE1 || NETCORE50 || NETSTANDARD1_5 || NETSTANDARD1_6)
    public
#else
	internal
#endif
        class PostgreSqlFetchedJob : IFetchedJob
    {
        private readonly IDbConnection _connection;
        private readonly PostgreSqlStorageOptions _options;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;

        public PostgreSqlFetchedJob(
            IDbConnection connection,
            PostgreSqlStorageOptions options,
            int id,
            string jobId,
            string queue)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _options = options ?? throw new ArgumentNullException(nameof(options));

            Id = id;
            JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
            Queue = queue ?? throw new ArgumentNullException(nameof(queue));
        }

        public int Id { get; }
        public string JobId { get; }
        public string Queue { get; }

        public void RemoveFromQueue()
        {
            var sql = $@"
DELETE FROM ""{_options.SchemaName}"".""jobqueue"" 
WHERE ""id"" = @id;
";
            _connection.Execute(sql, new {id = Id});
            _removedFromQueue = true;
        }

        public void Requeue()
        {
            var sql = $@"
UPDATE ""{_options.SchemaName}"".""jobqueue"" 
SET ""fetchedat"" = NULL 
WHERE ""id"" = @id;
";
            _connection.Execute(sql, new {id = Id});
            _requeued = true;
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
