using System;
using System.Diagnostics;
using System.Threading;
using Dapper;

// ReSharper disable RedundantAnonymousTypePropertyName
namespace Hangfire.PostgreSql
{
    internal class PostgreSqlDistributedLock : IDisposable
    {
        private static readonly ThreadLocal<Random> Random = new ThreadLocal<Random>(() => new Random());

        private readonly string _resource;
        private readonly TimeSpan _timeout;
        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private bool _completed;

        public PostgreSqlDistributedLock(string resource,
            TimeSpan timeout,
            IPostgreSqlConnectionProvider connectionProvider)
        {
            Guard.ThrowIfNullOrEmpty(resource, nameof(resource));
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));

            _resource = resource;
            _timeout = timeout;
            _connectionProvider = connectionProvider;

            Initialize();
        }

        private void Initialize()
        {
            var lockAcquiringWatch = Stopwatch.StartNew();
            var tryAcquireLock = true;
            var sleepTime = 50;
            while (tryAcquireLock)
            {
                using (var connectionHolder = _connectionProvider.AcquireConnection())
                {
                    const string query = @"
INSERT INTO lock(resource, acquired) 
VALUES (@resource, current_timestamp at time zone 'UTC')
ON CONFLICT (resource) DO NOTHING
;";
                    var parameters = new { resource = _resource };
                    var rowsAffected = connectionHolder.Connection.Execute(query, parameters);

                    if (rowsAffected > 0) return;
                }

                tryAcquireLock = CheckAndWaitForNextTry(lockAcquiringWatch.ElapsedMilliseconds, ref sleepTime);
            }

            throw new PostgreSqlDistributedLockException(
                $"Could not place a lock on the resource \'{_resource}\': Lock timeout.");
        }

        private bool CheckAndWaitForNextTry(long elapsedMilliseconds, ref int sleepTime)
        {
            var maxSleepTimeMilliseconds = sleepTime * 2;
            var tryAcquireLock = true;
            var timeoutTotalMilliseconds = _timeout.TotalMilliseconds;
            if (elapsedMilliseconds > timeoutTotalMilliseconds)
            {
                tryAcquireLock = false;
            }
            else
            {
                var sleepDuration = timeoutTotalMilliseconds - elapsedMilliseconds;
                if (sleepDuration > maxSleepTimeMilliseconds) sleepDuration = maxSleepTimeMilliseconds;
                if (sleepDuration > 0)
                {
                    Thread.Sleep((int)sleepDuration);
                }
                else
                {
                    tryAcquireLock = false;
                }
            }

            sleepTime = Random.Value.Next(sleepTime, maxSleepTimeMilliseconds);
            return tryAcquireLock;
        }

        public void Dispose()
        {
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                if (_completed) return;
                _completed = true;

                const string query = @"
DELETE FROM lock 
WHERE resource = @resource;
";
                var rowsAffected = connectionHolder.Connection.Execute(query, new { resource = _resource });
                if (rowsAffected <= 0)
                {
                    throw new PostgreSqlDistributedLockException(
                        $"Could not release a lock on the resource '{_resource}'. Lock does not exists.");
                }
            }
        }
    }
}
