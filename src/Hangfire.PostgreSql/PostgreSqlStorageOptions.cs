using System;

namespace Hangfire.PostgreSql
{
    public class PostgreSqlStorageOptions
    {
        private TimeSpan _queuePollInterval;
        private TimeSpan _invisibilityTimeout;
        private TimeSpan _distributedLockTimeout;
        private uint _connectionsCount;

        public PostgreSqlStorageOptions()
        {
            QueuePollInterval = TimeSpan.FromSeconds(15);
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
                ThrowIfValueIsNotPositive(value, nameof(QueuePollInterval));
                _queuePollInterval = value;
            }
        }

        public TimeSpan InvisibilityTimeout
        {
            get => _invisibilityTimeout;
            set
            {
                ThrowIfValueIsNotPositive(value, nameof(InvisibilityTimeout));
                _invisibilityTimeout = value;
            }
        }

        public TimeSpan DistributedLockTimeout
        {
            get => _distributedLockTimeout;
            set
            {
                ThrowIfValueIsNotPositive(value, nameof(DistributedLockTimeout));
                _distributedLockTimeout = value;
            }
        }

        public uint ConnectionsCount
        {
            get => _connectionsCount;
            set
            {
                if (value == 0) throw new ArgumentException("Cannot be zero", nameof(ConnectionsCount));
                _connectionsCount = value;
            }
        }

        public bool PrepareSchemaIfNecessary { get; set; }

        public string SchemaName { get; set; }


        private static void ThrowIfValueIsNotPositive(TimeSpan value, string fieldName)
        {
            var message = $"The {fieldName} property value should be positive. Given: {value}.";

            if (value == TimeSpan.Zero)
            {
                throw new ArgumentException(message, nameof(value));
            }
            if (value != value.Duration())
            {
                throw new ArgumentException(message, nameof(value));
            }
        }
    }
}
