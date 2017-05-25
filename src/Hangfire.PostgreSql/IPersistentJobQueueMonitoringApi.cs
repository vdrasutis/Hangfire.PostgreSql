using System.Collections.Generic;
using Hangfire.Storage.Monitoring;

namespace Hangfire.PostgreSql
{
    internal interface IPersistentJobQueueMonitoringApi
    {
        IEnumerable<string> GetQueues();
        JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int @from, int perPage);
        JobList<FetchedJobDto> FetchedJobs(string queue, int @from, int perPage);
        (long? enqueued, long? fetched) GetEnqueuedAndFetchedCount(string queue);
    }
}
