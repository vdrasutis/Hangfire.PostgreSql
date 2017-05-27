using System.Threading;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    internal interface IPersistentJobQueue
    {
        void Enqueue(string queue, string jobId);
        void Enqueue(string queue, string jobId, NpgsqlConnection connection);
        IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);
    }
}
