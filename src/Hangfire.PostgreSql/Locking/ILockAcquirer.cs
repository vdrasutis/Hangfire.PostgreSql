using System;

namespace Hangfire.PostgreSql.Locking
{
    internal interface ILockAcquirer
    {
        LockHolder AcquireLock(string resource, TimeSpan timeout);
    }
}
