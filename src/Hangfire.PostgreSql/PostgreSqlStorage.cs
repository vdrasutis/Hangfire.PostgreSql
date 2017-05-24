using System;
using System.Collections.Generic;
using Dapper;
using Hangfire.Dashboard;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    public class PostgreSqlStorage : JobStorage
    {
        private readonly PostgreSqlStorageOptions _options;
        private readonly PostgreSqlConnectionProvider _connectionProvider;
        private readonly string _storageInfo;

        /// <summary>
        /// Initializes PostgreSqlStorage with default PostgreSqlStorageOptions and either the provided connection
        /// string or the connection string with provided name pulled from the application config file.
        /// </summary>
        /// <param name="connectionString">A Postgres connection string</param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> argument is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="connectionString"/> argument is a valid 
        /// Postgres connection string </exception>
        public PostgreSqlStorage(string connectionString)
            : this(connectionString, new PostgreSqlStorageOptions())
        {
        }

        /// <summary>
        /// Initializes PostgreSqlStorage from the provided PostgreSqlStorageOptions and either the provided connection
        /// string or the connection string with provided name pulled from the application config file.       
        /// </summary>
        /// <param name="connectionString">A Postgres connection string</param>
        /// <param name="options"></param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> argument is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="connectionString"/> argument is a valid 
        /// Postgres connection string </exception>
        public PostgreSqlStorage(string connectionString, PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(connectionString, nameof(connectionString));
            Guard.ThrowIfNull(options, nameof(options));
            Guard.ThrowIfConnectionStringIsInvalid(connectionString);

            _options = options;
            _connectionProvider = new PostgreSqlConnectionProvider(connectionString, _options);

            var builder = new NpgsqlConnectionStringBuilder(connectionString);
            _storageInfo = $"PostgreSQL Server: Host: {builder.Host}, DB: {builder.Database}, Schema: {_options.SchemaName}";
            QueueProviders = new PersistentJobQueueProviderCollection(new PostgreSqlJobQueueProvider(_connectionProvider, _options));

            if (_options.PrepareSchemaIfNecessary)
            {
                _connectionProvider.Execute(
                    connection => PostgreSqlObjectsInstaller.Install(connection, _options.SchemaName));
            }
        }

        internal PersistentJobQueueProviderCollection QueueProviders { get; }

        internal IPostgreSqlConnectionProvider ConnectionProvider => _connectionProvider;

        internal PostgreSqlStorageOptions Options => _options;

        public override IMonitoringApi GetMonitoringApi()
            => new PostgreSqlMonitoringApi(_connectionProvider, _options, QueueProviders);

        public override IStorageConnection GetConnection()
            => new PostgreSqlConnection(_connectionProvider, QueueProviders, _options);

        public override IEnumerable<IServerComponent> GetComponents()
            => new IServerComponent[]
            {
                new ExpirationManager(_connectionProvider, _options),
                new CountersAggregationManager(_connectionProvider, _options)
            };

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.InfoFormat("    Queue poll interval: {0}.", _options.QueuePollInterval);
            logger.InfoFormat("    Invisibility timeout: {0}.", _options.InvisibilityTimeout);
            logger.InfoFormat("    Distributed lock timeout: {0}.", _options.DistributedLockTimeout);
        }

        public override string ToString() => _storageInfo;

        public static readonly DashboardMetric MaxConnections = new DashboardMetric(
            "connections:max",
            "Max Connections",
            page => GetMetricByQuery(page, @"SHOW max_connections;"));

        public static readonly DashboardMetric ActiveConnections = new DashboardMetric(
            "connections:active",
            "Active Connections",
            page => GetMetricByQuery(page, @"SELECT numbackends from pg_stat_database WHERE datname = current_database();"));

        public static readonly DashboardMetric LocksCount = new DashboardMetric(
            "locks:count",
            "Locks Count",
            page => GetMetricByQuery(page, @"SELECT COUNT(*) FROM pg_locks;"));

        public static readonly DashboardMetric PostgreSqlServerVersion = new DashboardMetric(
            "server:version",
            "PostgreSql Version",
            page => GetMetricByQuery(page, @"SHOW server_version;"));

        public static readonly DashboardMetric CacheHitsPerRead = new DashboardMetric(
            "cache:hitratio",
            "Cache Hits Per Read",
            page => GetMetricByQuery(page, @"SELECT ROUND(SUM(blks_hit) / SUM(blks_read)) FROM pg_stat_database;"));

        private static Metric GetMetricByQuery(RazorPage page, string query)
        {
            var storage = page.Storage as PostgreSqlStorage;
            if (storage == null) return new Metric("???");

            using (var connectionHolder = storage.ConnectionProvider.AcquireConnection())
            {
                var serverVersion = connectionHolder.Connection.ExecuteScalar(query);
                return new Metric(serverVersion?.ToString() ?? "???");
            }
        }
    }
}
