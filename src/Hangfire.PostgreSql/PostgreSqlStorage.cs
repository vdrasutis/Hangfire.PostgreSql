using System;
using System.Collections.Generic;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    public class PostgreSqlStorage : JobStorage
    {
        private readonly PostgreSqlStorageOptions _options;
        private readonly PostgreSqlConnectionProvider _postgreSqlConnectionProvider;
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
            _postgreSqlConnectionProvider = new PostgreSqlConnectionProvider(connectionString, _options);

            var builder = new NpgsqlConnectionStringBuilder(connectionString);
            _storageInfo = $"PostgreSQL Server: Host: {builder.Host}, DB: {builder.Database}, Schema: {_options.SchemaName}";
            QueueProviders = new PersistentJobQueueProviderCollection(new PostgreSqlJobQueueProvider(_postgreSqlConnectionProvider, _options));

            if (_options.PrepareSchemaIfNecessary)
            {
                _postgreSqlConnectionProvider.Execute(
                    connection => PostgreSqlObjectsInstaller.Install(connection, _options.SchemaName));
            }
        }

        internal PersistentJobQueueProviderCollection QueueProviders { get; }

        public override IMonitoringApi GetMonitoringApi()
            => new PostgreSqlMonitoringApi(_postgreSqlConnectionProvider, _options, QueueProviders);

        public override IStorageConnection GetConnection()
            => new PostgreSqlConnection(_postgreSqlConnectionProvider, QueueProviders, _options);

        public override IEnumerable<IServerComponent> GetComponents()
            => new IServerComponent[]
            {
                new ExpirationManager(_postgreSqlConnectionProvider, _options),
                new CountersAggregationManager(_postgreSqlConnectionProvider, _options)
            };

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.InfoFormat("    Queue poll interval: {0}.", _options.QueuePollInterval);
            logger.InfoFormat("    Invisibility timeout: {0}.", _options.InvisibilityTimeout);
            logger.InfoFormat("    Distributed lock timeout: {0}.", _options.DistributedLockTimeout);
        }

        public override string ToString() => _storageInfo;
    }
}
