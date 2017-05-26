using System;
using System.Data;
using System.Diagnostics;
using System.Threading.Tasks;
using Dapper;
using Npgsql;

// ReSharper disable RedundantAnonymousTypePropertyName
namespace Hangfire.PostgreSql
{
    internal class PostgreSqlDistributedLock : IDisposable
    {
        private readonly string _resource;
        private readonly TimeSpan _timeout;
        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private readonly PostgreSqlStorageOptions _options;
        private bool _completed;

        public PostgreSqlDistributedLock(string resource,
            TimeSpan timeout,
            IPostgreSqlConnectionProvider connectionProvider,
            PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNullOrEmpty(resource, nameof(resource));
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(options, nameof(options));

            _resource = resource;
            _timeout = timeout;
            _connectionProvider = connectionProvider;
            _options = options;

            Initialize();
        }

        private void Initialize()
        {
            var lockAcquiringWatch = Stopwatch.StartNew();
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var connection = connectionHolder.Connection;
                bool tryAcquireLock = true;
                while (tryAcquireLock)
                {
                    TryRemoveTimeoutedLock(connection);
                    try
                    {
                        int rowsAffected;
                        using (var transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead))
                        {
                            var sql = $@"
INSERT INTO ""{_options.SchemaName}"".lock(resource, acquired) 
SELECT @resource, current_timestamp at time zone 'UTC'
WHERE NOT EXISTS (
    SELECT 1 FROM ""{_options.SchemaName}"".lock
    WHERE resource = @resource
);";
                            var parameters = new
                            {
                                resource = _resource
                            };
                            rowsAffected = connection.Execute(sql, parameters, transaction);
                            transaction.Commit();
                        }
                        if (rowsAffected > 0) return;
                    }
                    catch (Exception e)
                    {
                        throw new PostgreSqlDistributedLockException(
                            $"Could not place a lock on the resource \'{_resource}\'. See inner exception", e);
                    }

                    tryAcquireLock = CheckAndWaitForNextTry(lockAcquiringWatch.ElapsedMilliseconds);
                }
            }

            throw new PostgreSqlDistributedLockException(
                $"Could not place a lock on the resource \'{_resource}\': Lock timeout.");
        }

        private bool CheckAndWaitForNextTry(long elapsedMilliseconds)
        {
            const int maxSleepTimeMilliseconds = 500;
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
                    Task.Delay((int)sleepDuration).Wait();
                }
                else
                {
                    tryAcquireLock = false;
                }
            }
            return tryAcquireLock;
        }

        private void TryRemoveTimeoutedLock(NpgsqlConnection connection)
        {
            try
            {
                using (var transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead))
                {
                    var query = $@"
DELETE FROM ""{_options.SchemaName}"".lock
WHERE resource = @resource
AND acquired < current_timestamp at time zone 'UTC' - @timeout";

                    var parameters = new
                    {
                        resource = _resource,
                        timeout = _options.DistributedLockTimeout
                    };
                    connection.Execute(query, parameters, transaction);
                    transaction.Commit();
                }
            }
            catch (Exception e)
            {
                throw new PostgreSqlDistributedLockException(
                    $"Could not remove timeouted lock on the resource \'{_resource}\'. See inner exception", e);
            }
        }

        public void Dispose()
        {
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                if (_completed) return;
                _completed = true;

                var query = $@"
DELETE FROM ""{_options.SchemaName}"".lock 
WHERE ""resource"" = @resource;
";
                int rowsAffected = connectionHolder.Connection.Execute(query, new { resource = _resource });
                if (rowsAffected <= 0)
                {
                    throw new PostgreSqlDistributedLockException(
                        $"Could not release a lock on the resource '{_resource}'. Lock does not exists.");
                }
            }
        }
    }
}
