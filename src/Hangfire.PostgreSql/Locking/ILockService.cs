namespace Hangfire.PostgreSql.Locking
{
    internal interface ILockService : ILockAcquirer
    {
        string AcquirerId { get; }
        
        void ReleaseLock(LockHolder lockHolder);
    }
}
