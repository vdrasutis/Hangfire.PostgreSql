using System;

// ReSharper disable MemberCanBePrivate.Global
namespace Hangfire.PostgreSql
{
    [Serializable]
    public class PostgreSqlDistributedLockException : Exception
    {
        public PostgreSqlDistributedLockException() { }

        public PostgreSqlDistributedLockException(string message) 
            : base(message) { }

        public PostgreSqlDistributedLockException(string message, Exception innerException) 
            : base(message, innerException) { }
    }
}
