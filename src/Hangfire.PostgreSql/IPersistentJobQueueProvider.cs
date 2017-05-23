using System.Data;

namespace Hangfire.PostgreSql
{
    public interface IPersistentJobQueueProvider
    {
        IPersistentJobQueue GetJobQueue(IDbConnection connection);
        IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi(IDbConnection connection);
    }
}
