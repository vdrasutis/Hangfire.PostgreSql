using System.Threading;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
    internal interface IPersistentJobQueue
    {
        void Enqueue(string queue, string jobId);
        IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);
    }
}
