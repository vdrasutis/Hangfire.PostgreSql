using System;

namespace Hangfire.PostgreSql
{
    public class PostgreSqlStorageOptions
    {
        private TimeSpan _queuePollInterval;
        private TimeSpan _invisibilityTimeout;
        private TimeSpan _distributedLockTimeout;
        private int _connectionsCount;

        public PostgreSqlStorageOptions()
        {
            QueuePollInterval = TimeSpan.FromMilliseconds(300);
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
            DistributedLockTimeout = TimeSpan.FromMinutes(10);
            ConnectionsCount = 10;
            SchemaName = "hangfire";
            PrepareSchemaIfNecessary = true;
        }

        public TimeSpan QueuePollInterval
        {
            get => _queuePollInterval;
            set
            {
                Guard.ThrowIfValueIsNotPositive(value, nameof(QueuePollInterval));
                _queuePollInterval = value;
            }
        }

        public TimeSpan InvisibilityTimeout
        {
            get => _invisibilityTimeout;
            set
            {
                Guard.ThrowIfValueIsNotPositive(value, nameof(InvisibilityTimeout));
                _invisibilityTimeout = value;
            }
        }

        public TimeSpan DistributedLockTimeout
        {
            get => _distributedLockTimeout;
            set
            {
                Guard.ThrowIfValueIsNotPositive(value, nameof(DistributedLockTimeout));
                _distributedLockTimeout = value;
            }
        }

        public int ConnectionsCount
        {
            get => _connectionsCount;
            set
            {
                Guard.ThrowIfValueIsNotPositive(value, nameof(ConnectionsCount));
                _connectionsCount = value;
            }
        }

        public bool PrepareSchemaIfNecessary { get; set; }

        [Obsolete("This field will be removed in next version. Please use Search Path parameter in NpgSql connection string http://www.npgsql.org/doc/connection-string-parameters.html. Currently value of this parameter will be used for overwriting Search Path.")]
        public string SchemaName { get; set; }
    }
}
