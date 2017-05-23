using System;

namespace Hangfire.PostgreSql
{
#if (NETCORE1 || NETCORE50 || NETSTANDARD1_5 || NETSTANDARD1_6)
    public
#else
    [Serializable]
	internal
#endif
        class PostgreSqlDistributedLockException : Exception
    {
        public PostgreSqlDistributedLockException(string message)
            : base(message)
        {
        }
    }
}
