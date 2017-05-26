using System;
using System.Data;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.PostgreSql
{
#pragma warning disable 618 // TODO Remove when Hangfire 2.0 will be released
    internal sealed class CountersAggregationManager : IBackgroundProcess, IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpirationManager));

        private static readonly string[] ProcessedCounters =
        {
            "stats:succeeded",
            "stats:deleted",
        };

        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private readonly PostgreSqlStorageOptions _options;
        private readonly TimeSpan _checkInterval;

        public CountersAggregationManager(IPostgreSqlConnectionProvider connectionProvider, PostgreSqlStorageOptions options)
            : this(connectionProvider, options, TimeSpan.FromHours(1))
        {
        }

        public CountersAggregationManager(IPostgreSqlConnectionProvider connectionProvider, PostgreSqlStorageOptions options, TimeSpan checkInterval)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(options, nameof(options));
            Guard.ThrowIfValueIsNotPositive(checkInterval, nameof(checkInterval));

            _connectionProvider = connectionProvider;
            _options = options;
            _checkInterval = checkInterval;
        }

        public override string ToString() => "PostgreSql Counters Aggregation Manager";

        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }

        public void Execute(CancellationToken cancellationToken)
        {
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
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            using (var transaction = connectionHolder.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                var aggregateQuery = $@"
WITH counters AS (
DELETE FROM ""{_options.SchemaName}"".""counter""
WHERE key = '{counterName}'
AND expireat IS NULL
RETURNING *
)

SELECT SUM(value) FROM counters;
";

                var aggregatedValue = connectionHolder.Connection.ExecuteScalar<long>(aggregateQuery, transaction: transaction);
                transaction.Commit();

                if (aggregatedValue > 0)
                {
                    var query = $@"INSERT INTO ""{_options.SchemaName}"".counter (key, value) VALUES (@key, @value);";
                    connectionHolder.Connection.Execute(query, new { key = counterName, value = aggregatedValue });
                }
                Logger.InfoFormat("Aggregated counter \'{0}\', value: {1}", counterName, aggregatedValue);
            }
        }
    }
}
