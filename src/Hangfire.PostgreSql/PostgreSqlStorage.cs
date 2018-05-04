using System;
using System.Collections.Generic;
using Hangfire.Logging;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    public sealed class PostgreSqlStorage : JobStorage
    {
        private readonly PostgreSqlStorageOptions _options;
        private readonly IConnectionProvider _connectionProvider;
        private readonly string _storageInfo;
        private readonly PostgreSqlConnection _postgreSqlConnection;
        private readonly MonitoringApi _monitoringApi;

        /// <summary>
        /// Initializes PostgreSqlStorage with the provided connection string and default PostgreSqlStorageOptions.
        /// </summary>
        /// <param name="connectionString">A Postgres connection string</param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="connectionString"/> is not valid PostgreSql connection string </exception>
        public PostgreSqlStorage(string connectionString)
            : this(connectionString, new PostgreSqlStorageOptions())
        {
        }

        /// <summary>
        /// Initializes PostgreSqlStorage with the provided connection string and the provided PostgreSqlStorageOptions.
        /// </summary>
        /// <param name="connectionString">A Postgres connection string</param>
        /// <param name="options"></param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="connectionString"/> is not valid PostgreSql connection string.</exception>
        public PostgreSqlStorage(string connectionString, PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(connectionString, nameof(connectionString));
            Guard.ThrowIfNull(options, nameof(options));
            Guard.ThrowIfConnectionStringIsInvalid(connectionString);

            _options = options;

            var builder = new NpgsqlConnectionStringBuilder(connectionString);
            _connectionProvider = CreateConnectionProvider(connectionString, builder);

            var queue = new PostgreSqlJobQueue(_connectionProvider, _options);
            var queueMonitoringApi = new JobQueueMonitoringApi(_connectionProvider, _options);
            _postgreSqlConnection = new PostgreSqlConnection(_connectionProvider, queue, _options);
            _monitoringApi = new MonitoringApi(_connectionProvider, queueMonitoringApi);
            _storageInfo = $"PostgreSQL Server: Host: {builder.Host}, DB: {builder.Database}, Schema: {builder.SearchPath}";

            PrepareSchemaIfNecessary();
        }

        private static IConnectionProvider CreateConnectionProvider(string connectionString, NpgsqlConnectionStringBuilder connectionStringBuilder)
        {
            return connectionStringBuilder.Pooling
                ? (IConnectionProvider)new DefaultConnectionProvider(connectionString)
                : new NpgsqlConnectionProvider(connectionString);
        }

        private void PrepareSchemaIfNecessary()
        {
            if (_options.PrepareSchemaIfNecessary)
            {
                using (var connectionHolder = _connectionProvider.AcquireConnection())
                {
                    DatabaseInitializer.Initialize(connectionHolder.Connection);
                }
            }
        }

        internal IConnectionProvider ConnectionProvider => _connectionProvider;

        public override IMonitoringApi GetMonitoringApi() => _monitoringApi;

        public override IStorageConnection GetConnection() => _postgreSqlConnection;

#pragma warning disable 618 // TODO Remove when Hangfire 2.0 will be released
        public override IEnumerable<IServerComponent> GetComponents()
            => new IServerComponent[]
#pragma warning restore 618
            {
                new ExpirationManager(_connectionProvider),
                new ExpiredLocksManager(_connectionProvider, _options.DistributedLockTimeout),
                new CountersAggregationManager(_connectionProvider)
            };

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.Info(_storageInfo);
            logger.InfoFormat("    Prepare schema: {0}.", _options.PrepareSchemaIfNecessary);
            logger.InfoFormat("    Queue poll interval: {0}.", _options.QueuePollInterval);
            logger.InfoFormat("    Invisibility timeout: {0}.", _options.InvisibilityTimeout);
            logger.InfoFormat("    Distributed lock timeout: {0}.", _options.DistributedLockTimeout);
        }

        public override string ToString() => _storageInfo;
    }
}
