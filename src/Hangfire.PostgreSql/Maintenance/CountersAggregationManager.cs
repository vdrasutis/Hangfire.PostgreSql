using System;
using System.Data;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Locking;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.PostgreSql.Maintenance
{
#pragma warning disable 618 // TODO Remove when Hangfire 2.0 will be released
    internal sealed class CountersAggregationManager : IBackgroundProcess, IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(CountersAggregationManager));

        private static readonly string[] ProcessedCounters =
        {
            "stats:succeeded",
            "stats:deleted"
        };

        private readonly IConnectionProvider _connectionProvider;
        private readonly ILockService _lockService;
        private readonly TimeSpan _checkInterval;

        public CountersAggregationManager(IConnectionProvider connectionProvider, ILockService lockService)
            : this(connectionProvider, lockService, TimeSpan.FromSeconds(30)) { }

        public CountersAggregationManager(IConnectionProvider connectionProvider, ILockService lockService,
            TimeSpan checkInterval)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(lockService, nameof(lockService));
            Guard.ThrowIfValueIsNotPositive(checkInterval, nameof(checkInterval));

            _connectionProvider = connectionProvider;
            _lockService = lockService;
            _checkInterval = checkInterval;
        }

        public override string ToString() => "PostgreSQL Counters Aggregation Manager";

        public void Execute(BackgroundProcessContext context) => Execute(context.StoppingToken);

        public void Execute(CancellationToken cancellationToken)
        {
            AggregateCounters(cancellationToken);
            cancellationToken.Wait(_checkInterval);
        }

        private void AggregateCounters(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var processedCounter in ProcessedCounters)
            {
                try
                {
                    using var _ = _lockService.AcquireLock("counters:aggregation", TimeSpan.FromSeconds(1));

                    AggregateCounter(processedCounter);
                }
                catch (DistributedLockTimeoutException)
                {
                    // means that someone already aggregating counters
                }

                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        private void AggregateCounter(string counterName)
        {
            using var connectionHolder = _connectionProvider.AcquireConnection();
            using var transaction = connectionHolder.Connection.BeginTransaction(IsolationLevel.ReadCommitted);
            const string aggregateQuery = @"
with aggregated_counters as (
    delete from counter
    where key = @counterName
    and expireat is null
    returning *
)

select sum(value) from aggregated_counters;
";

            var aggregatedValue =
                connectionHolder.Connection.ExecuteScalar<long>(aggregateQuery, new { counterName }, transaction);

            if (aggregatedValue > 0)
            {
                const string query = @"insert into counter (key, value) values (@key, @value);";
                connectionHolder.Connection.Execute(query, new { key = counterName, value = aggregatedValue }, transaction);
            }

            transaction.Commit();

            Logger.InfoFormat("Aggregated counter \'{0}\', value: {1}", counterName, aggregatedValue);
        }
    }
}
