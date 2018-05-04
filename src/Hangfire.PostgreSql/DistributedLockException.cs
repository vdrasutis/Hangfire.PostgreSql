using System;

// ReSharper disable MemberCanBePrivate.Global
namespace Hangfire.PostgreSql
{
    [Serializable]
    public class DistributedLockException : Exception
    {
        public DistributedLockException() { }

        public DistributedLockException(string message) 
            : base(message) { }

        public DistributedLockException(string message, Exception innerException) 
            : base(message, innerException) { }
    }
}
