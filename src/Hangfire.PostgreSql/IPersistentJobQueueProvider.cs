namespace Hangfire.PostgreSql
{
    internal interface IPersistentJobQueueProvider
    {
        IPersistentJobQueue GetJobQueue(IPostgreSqlConnectionProvider connectionProvider);
        IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi(IPostgreSqlConnectionProvider connection);
    }
}
