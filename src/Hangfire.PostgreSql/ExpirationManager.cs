using System;
using System.Data;
using System.Globalization;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.PostgreSql
{
    public class ExpirationManager : IBackgroundProcess, IServerComponent
    {
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private const int NumberOfRecordsInSinglePass = 1000;

        private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpirationManager));

        private static readonly string[] ProcessedCounters =
        {
            "stats:succeeded",
            "stats:deleted",
        };

        private static readonly string[] ProcessedTables =
        {
            "counter",
            "job",
            "list",
            "set",
            "hash",
        };

        private readonly PostgreSqlStorage _storage;
        private readonly PostgreSqlStorageOptions _options;
        private readonly TimeSpan _checkInterval;

        public ExpirationManager(PostgreSqlStorage storage, PostgreSqlStorageOptions options)
            : this(storage, options, TimeSpan.FromHours(1))
        {
        }

        public ExpirationManager(PostgreSqlStorage storage, PostgreSqlStorageOptions options, TimeSpan checkInterval)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _checkInterval = checkInterval;
        }

        public override string ToString() => "SQL Records Expiration Manager";

        public void Execute(BackgroundProcessContext context) => Execute(context.CancellationToken);

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (var table in ProcessedTables)
            {
                Logger.DebugFormat("Removing outdated records from table '{0}'...", table);

                int removedCount = 0;

                do
                {
                    using (var storageConnection = (PostgreSqlConnection) _storage.GetConnection())
                    {
                        using (var transaction =
                            storageConnection.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
                        {
                            removedCount = storageConnection.Connection.Execute(
                                string.Format(@"
DELETE FROM """ + _options.SchemaName + @""".""{0}"" 
WHERE ""id"" IN (
    SELECT ""id"" 
    FROM """ + _options.SchemaName + @""".""{0}"" 
    WHERE ""expireat"" < NOW() AT TIME ZONE 'UTC' 
    LIMIT {1}
)", table, NumberOfRecordsInSinglePass.ToString(CultureInfo.InvariantCulture)), transaction);

                            transaction.Commit();
                        }
                    }

                    if (removedCount > 0)
                    {
                        Logger.InfoFormat("Removed {0} outdated record(s) from '{1}' table.", removedCount, table);

                        cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                } while (removedCount != 0);
            }
            AggregateCounters(cancellationToken);
            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        private void AggregateCounters(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var processedCounter in ProcessedCounters)
            {
                AggregateCounter(processedCounter);
                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        private void AggregateCounter(string counterName)
        {
            using (var connection = (PostgreSqlConnection) _storage.GetConnection())
            {
                using (var transaction = connection.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    var aggregateQuery = $@"
WITH counters AS (
DELETE FROM ""{_options.SchemaName}"".""counter""
WHERE ""key"" = '{counterName}'
AND ""expireat"" IS NULL
RETURNING *
)

SELECT SUM(value) FROM counters;
";

                    var aggregatedValue =
                        connection.Connection.ExecuteScalar<long>(aggregateQuery, transaction: transaction);
                    transaction.Commit();

                    if (aggregatedValue > 0)
                    {
                        var insertQuery =
                            $@"INSERT INTO ""{
                                    _options.SchemaName
                                }"".""counter""(""key"", ""value"") VALUES (@key, @value);";
                        connection.Connection.Execute(insertQuery, new {key = counterName, value = aggregatedValue});
                    }
                }
            }
        }
    }
}
