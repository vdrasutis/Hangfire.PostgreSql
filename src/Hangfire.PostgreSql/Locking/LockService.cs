using System;
using System.Diagnostics;
using System.Threading;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.PostgreSql.Locking
{
#pragma warning disable 618 // TODO: remove IServerComponent when migrating to Hangfire 2
    internal sealed class LockService : ILockService, IBackgroundProcess, IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(LockService));

        private readonly ThreadLocal<Random> _random;
        private readonly IConnectionProvider _connectionProvider;
        private readonly TimeSpan _distributedLockTimeout;
        private readonly string _acquirerId = Guid.NewGuid().ToString("N");

        public LockService(IConnectionProvider connectionProvider, TimeSpan distributedLockTimeout)
        {
            _random = new ThreadLocal<Random>(() => new Random());
            _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
            _distributedLockTimeout = distributedLockTimeout;
        }

        public override string ToString() => "PostgreSQL Locks Service";

        LockHolder ILockAcquirer.AcquireLock(string resource, TimeSpan timeout)
        {
            Guard.ThrowIfNullOrEmpty(resource, nameof(resource));
            Guard.ThrowIfValueIsNegative(timeout, nameof(timeout));

            var sleepTime = 50;
            var lockAcquiringWatch = Stopwatch.StartNew();

            using var connectionHolder = _connectionProvider.AcquireConnection();
            do
            {
                const string query = @"
insert into lock(resource, acquired, acquirer) 
values (@resource, current_timestamp at time zone 'UTC', @lockHolder)
on conflict (resource) do nothing
;";
                // ReSharper disable once RedundantAnonymousTypePropertyName
                var parameters = new
                {
                    resource = resource,
                    lockHolder = _acquirerId
                };
                var rowsAffected = connectionHolder.Execute(query, parameters);

                if (rowsAffected > 0)
                {
                    return new LockHolder(resource, this);
                }
            } while (IsNotTimeout(lockAcquiringWatch.Elapsed, timeout, ref sleepTime));

            throw new DistributedLockTimeoutException(resource);
        }

        private bool IsNotTimeout(TimeSpan elapsed, TimeSpan timeout, ref int sleepTime)
        {
            if (elapsed > timeout)
            {
                return false;
            }

            Thread.Sleep(sleepTime);

            var maxSleepTimeMilliseconds = Math.Min(sleepTime * 2, 1000);
            sleepTime = _random.Value.Next(sleepTime, maxSleepTimeMilliseconds);

            return true;
        }

        string ILockService.AcquirerId => _acquirerId;

        void ILockService.ReleaseLock(LockHolder lockHolder)
        {
            if (lockHolder.Disposed) return;

            using var connectionHolder = _connectionProvider.AcquireConnection();

            const string query = @"
delete from lock 
where resource = @resource
and acquirer = @lockHolder;
";
            var rowsAffected = connectionHolder.Execute(query,
                new
                {
                    resource = lockHolder.Resource,
                    lockHolder = _acquirerId
                });

            if (rowsAffected <= 0)
            {
                throw new DistributedLockException(
                    $"Could not release a lock on the resource '{lockHolder.Resource}'. Lock does not exists.");
            }
        }

        public void Execute(BackgroundProcessContext context) => Execute(context.StoppingToken);

        public void Execute(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            const string query = @"
delete from lock
where acquired < current_timestamp at time zone 'UTC' - @timeout
and acquirer not in (select lockAcquirerId from server)
";

            var locksRemoved = _connectionProvider.Execute(query, new { timeout = _distributedLockTimeout });
            if (locksRemoved > 0)
            {
                Logger.InfoFormat("{0} expired locks removed.", locksRemoved);
            }

            cancellationToken.Wait(_distributedLockTimeout);
        }
    }
}
