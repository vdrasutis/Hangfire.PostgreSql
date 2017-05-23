using System;
using System.Data;

namespace Hangfire.PostgreSql
{
#if (NETCORE1 || NETCORE50 || NETSTANDARD1_5 || NETSTANDARD1_6)
    public
#else
	internal
#endif
        class PostgreSqlJobQueueProvider : IPersistentJobQueueProvider
    {
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlJobQueueProvider(PostgreSqlStorageOptions options)
        {
            if (options == null) throw new ArgumentNullException("options");
            _options = options;
        }

        public PostgreSqlStorageOptions Options
        {
            get { return _options; }
        }

        public IPersistentJobQueue GetJobQueue(IDbConnection connection)
        {
            return new PostgreSqlJobQueue(connection, _options);
        }

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi(IDbConnection connection)
        {
            return new PostgreSqlJobQueueMonitoringApi(connection, _options);
        }
    }
}
