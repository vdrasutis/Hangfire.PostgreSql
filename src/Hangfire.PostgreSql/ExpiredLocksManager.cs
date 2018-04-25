using System.Threading;
using Dapper;
using Hangfire.Server;

namespace Hangfire.PostgreSql
{
#pragma warning disable 618 // TODO: remove IServerComponent when migrating to Hangfire 2
    public class ExpiredLocksManager : IBackgroundProcess, IServerComponent
#pragma warning restore 618
    {
        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private readonly PostgreSqlStorageOptions _options;

        internal ExpiredLocksManager(IPostgreSqlConnectionProvider connectionProvider, PostgreSqlStorageOptions options)
        {
            _connectionProvider = connectionProvider;
            _options = options;
        }

        public void Execute(BackgroundProcessContext context) => Execute(context.CancellationToken);

        public void Execute(CancellationToken cancellationToken)
        {
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                const string query = @"
DELETE FROM lock
WHERE acquired < current_timestamp at time zone 'UTC' - @timeout";

                var parameters = new
                {
                    timeout = _options.DistributedLockTimeout
                };
                connectionHolder.Connection.Execute(query, parameters);
            }

            cancellationToken.WaitHandle.WaitOne(_options.DistributedLockTimeout);
        }
    }
}
