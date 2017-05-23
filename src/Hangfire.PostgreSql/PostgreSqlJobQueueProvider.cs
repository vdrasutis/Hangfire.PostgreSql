using System.Data;

namespace Hangfire.PostgreSql
{
    internal class PostgreSqlJobQueueProvider : IPersistentJobQueueProvider
    {
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlJobQueueProvider(PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(options, nameof(options));
            _options = options;
        }

        public PostgreSqlStorageOptions Options => _options;

        public IPersistentJobQueue GetJobQueue(IDbConnection connection)
            => new PostgreSqlJobQueue(connection, _options);

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi(IDbConnection connection)
            => new PostgreSqlJobQueueMonitoringApi(connection, _options);
    }
}
