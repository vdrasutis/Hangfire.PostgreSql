namespace Hangfire.PostgreSql
{
    internal class PostgreSqlJobQueueProvider : IPersistentJobQueueProvider
    {
        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlJobQueueProvider(IPostgreSqlConnectionProvider connectionProvider, PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(options, nameof(options));

            _connectionProvider = connectionProvider;
            _options = options;
        }

        public PostgreSqlStorageOptions Options => _options;

        public IPersistentJobQueue GetJobQueue(IPostgreSqlConnectionProvider connectionProvider)
            => new PostgreSqlJobQueue(_connectionProvider, _options);

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi(IPostgreSqlConnectionProvider connection)
            => new PostgreSqlJobQueueMonitoringApi(_connectionProvider, _options);
    }
}
