using System;
using System.Data;
using System.Globalization;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.PostgreSql
{
    internal sealed class ExpirationManager : IBackgroundProcess, IServerComponent
    {
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private const int NumberOfRecordsInSinglePass = 1000;

        private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpirationManager));

        private static readonly string[] ProcessedTables =
        {
            "counter",
            "job",
            "list",
            "set",
            "hash",
        };

        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private readonly PostgreSqlStorageOptions _options;
        private readonly TimeSpan _checkInterval;

        public ExpirationManager(IPostgreSqlConnectionProvider connectionProvider, PostgreSqlStorageOptions options)
            : this(connectionProvider, options, TimeSpan.FromHours(1))
        {
        }

        public ExpirationManager(IPostgreSqlConnectionProvider connectionProvider, PostgreSqlStorageOptions options, TimeSpan checkInterval)
        {
            _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _checkInterval = checkInterval;
        }

        public override string ToString() => "PostgreSql Expiration Manager";

        public void Execute(BackgroundProcessContext context) => Execute(context.CancellationToken);

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (var table in ProcessedTables)
            {
                Logger.DebugFormat("Removing outdated records from table '{0}'...", table);

                int removedCount = 0;

                do
                {
                    using (var connectionHolder = _connectionProvider.AcquireConnection())
                    using (var transaction = connectionHolder.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        var query = $@"
DELETE FROM ""{_options.SchemaName}"".{table} 
WHERE id IN (
    SELECT id
    FROM ""{_options.SchemaName}"".{table}
    WHERE expireat < NOW() AT TIME ZONE 'UTC' 
    LIMIT {NumberOfRecordsInSinglePass.ToString(CultureInfo.InvariantCulture)}
)";
                        removedCount = connectionHolder.Connection.Execute(query, transaction: transaction);
                        transaction.Commit();
                    }

                    if (removedCount > 0)
                    {
                        Logger.InfoFormat("Removed {0} outdated record(s) from '{1}' table.", removedCount, table);

                        cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                } while (removedCount != 0);
            }
            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }
    }
}
