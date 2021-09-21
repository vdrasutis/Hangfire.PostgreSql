using System;

namespace Hangfire.PostgreSql.Locking
{
    internal sealed class LockHolder : IDisposable
    {
        public string Resource { get; }
        public bool Disposed { get; private set; }
        private readonly ILockService _lockService;

        public LockHolder(string resource, ILockService lockService)
        {
            Disposed = false;
            Resource = resource;
            _lockService = lockService;
        }

        public void Dispose()
        {
            _lockService.ReleaseLock(this);
            Disposed = true;
        }
    }
}
