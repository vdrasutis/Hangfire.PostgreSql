using System;
using System.Collections.Generic;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    public sealed class PostgreSqlStorage : JobStorage
    {
        private readonly PostgreSqlStorageOptions _options;
        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private readonly string _storageInfo;
        private readonly PostgreSqlConnection _postgreSqlConnection;
        private readonly PostgreSqlMonitoringApi _postgreSqlMonitoringApi;

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
            _connectionProvider = new PostgreSqlConnectionProvider(connectionString, _options);

            var queue = new PostgreSqlJobQueue(_connectionProvider, _options);
            var queueMonitoringApi = new PostgreSqlJobQueueMonitoringApi(_connectionProvider, _options);
            _postgreSqlConnection = new PostgreSqlConnection(_connectionProvider, queue, _options);
            _postgreSqlMonitoringApi = new PostgreSqlMonitoringApi(_connectionProvider, queueMonitoringApi, _options);

            var builder = new NpgsqlConnectionStringBuilder(connectionString);
            _storageInfo = $"PostgreSQL Server: Host: {builder.Host}, DB: {builder.Database}, Schema: {_options.SchemaName}";
            if (_options.PrepareSchemaIfNecessary)
            {
                using (var connectionHolder = _connectionProvider.AcquireConnection())
                {
                    PostgreSqlObjectsInstaller.Install(connectionHolder.Connection, _options.SchemaName);
                }
            }
        }

        internal IPostgreSqlConnectionProvider ConnectionProvider => _connectionProvider;

        internal PostgreSqlStorageOptions Options => _options;

        public override IMonitoringApi GetMonitoringApi() => _postgreSqlMonitoringApi;

        public override IStorageConnection GetConnection() => _postgreSqlConnection;

#pragma warning disable 618 // TODO Remove when Hangfire 2.0 will be released
        public override IEnumerable<IServerComponent> GetComponents()
            => new IServerComponent[]
#pragma warning restore 618
            {
                new ExpirationManager(_connectionProvider, _options),
                new CountersAggregationManager(_connectionProvider, _options)
            };

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.InfoFormat("    Schema: {0}.", _options.SchemaName);
            logger.InfoFormat("    Prepare schema: {0}.", _options.PrepareSchemaIfNecessary);
            logger.InfoFormat("    Queue poll interval: {0}.", _options.QueuePollInterval);
            logger.InfoFormat("    Invisibility timeout: {0}.", _options.InvisibilityTimeout);
            logger.InfoFormat("    Distributed lock timeout: {0}.", _options.DistributedLockTimeout);
            logger.InfoFormat("    Connections count: {0}.", _options.ConnectionsCount);
        }

        public override string ToString() => _storageInfo;
    }
}
