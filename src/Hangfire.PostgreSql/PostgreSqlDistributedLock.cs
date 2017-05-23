using System;
using System.Data;
using System.Diagnostics;
using System.Threading.Tasks;
using Dapper;

namespace Hangfire.PostgreSql
{
    public sealed class PostgreSqlDistributedLock : IDisposable
    {
        private readonly string _resource;
        private readonly IDbConnection _connection;
        private readonly PostgreSqlStorageOptions _options;
        private bool _completed;

        public PostgreSqlDistributedLock(string resource, TimeSpan timeout, IDbConnection connection,
            PostgreSqlStorageOptions options)
        {
            if (string.IsNullOrEmpty(resource)) throw new ArgumentNullException(nameof(resource));
            _resource = resource;
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _options = options ?? throw new ArgumentNullException(nameof(options));

            PostgreSqlDistributedLock_Init_Transaction(resource, timeout, connection, options);
        }

        private static void PostgreSqlDistributedLock_Init_Transaction(string resource, TimeSpan timeout,
            IDbConnection connection, PostgreSqlStorageOptions options)
        {
            var lockAcquiringTime = Stopwatch.StartNew();

            bool tryAcquireLock = true;

            while (tryAcquireLock)
            {
                TryRemoveDeadlock(resource, connection, options);

                try
                {
                    int rowsAffected = -1;
                    using (var trx = connection.BeginTransaction(IsolationLevel.RepeatableRead))
                    {
                        rowsAffected = connection.Execute($@"
INSERT INTO ""{options.SchemaName}"".""lock""(""resource"", ""acquired"") 
SELECT @resource, @acquired
WHERE NOT EXISTS (
    SELECT 1 FROM ""{options.SchemaName}"".""lock"" 
    WHERE ""resource"" = @resource
);
",
                            new
                            {
                                resource = resource,
                                acquired = DateTime.UtcNow
                            }, trx);
                        trx.Commit();
                    }
                    if (rowsAffected > 0) return;
                }
                catch
                {
                }

                if (lockAcquiringTime.ElapsedMilliseconds > timeout.TotalMilliseconds)
                {
                    tryAcquireLock = false;
                }
                else
                {
                    int sleepDuration = (int) (timeout.TotalMilliseconds - lockAcquiringTime.ElapsedMilliseconds);
                    if (sleepDuration > 1000) sleepDuration = 1000;
                    if (sleepDuration > 0)
                    {
                        Task.Delay(sleepDuration).Wait();
                    }
                    else
                    {
                        tryAcquireLock = false;
                    }
                }
            }

            throw new PostgreSqlDistributedLockException(
                $"Could not place a lock on the resource \'{resource}\': Lock timeout.");
        }

        private static void TryRemoveDeadlock(string resource, IDbConnection connection,
            PostgreSqlStorageOptions options)
        {
            try
            {
                using (var transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead))
                {
                    int affected = -1;

                    affected = connection.Execute(
                        $@"DELETE FROM ""{
                                options.SchemaName
                            }"".""lock"" WHERE ""resource"" = @resource AND ""acquired"" < @timeout",
                        new
                        {
                            resource = resource,
                            timeout = DateTime.UtcNow - options.DistributedLockTimeout
                        });

                    transaction.Commit();
                }
            }
            catch
            {
            }
        }

        public void Dispose()
        {
            if (_completed) return;

            _completed = true;

            int rowsAffected = _connection.Execute($@"
DELETE FROM ""{_options.SchemaName}"".""lock"" 
WHERE ""resource"" = @resource;
",
                new
                {
                    resource = _resource
                });


            if (rowsAffected <= 0)
            {
                throw new PostgreSqlDistributedLockException(
                    $"Could not release a lock on the resource '{_resource}'. Lock does not exists.");
            }
        }
    }
}
