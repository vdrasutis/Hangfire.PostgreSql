using System;

namespace Hangfire.PostgreSql
{
    public class PostgreSqlStorageOptions
    {
        private TimeSpan _queuePollInterval;
        private TimeSpan _invisibilityTimeout;
        private TimeSpan _distributedLockTimeout;

        public PostgreSqlStorageOptions()
        {
            QueuePollInterval = TimeSpan.FromMilliseconds(300);
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
            DistributedLockTimeout = TimeSpan.FromMinutes(10);
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

        public bool PrepareSchemaIfNecessary { get; set; }
    }
}
