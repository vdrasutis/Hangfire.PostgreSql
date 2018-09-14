namespace Hangfire.PostgreSql.Entities
{
    public class EnqueuedAndFetchedJobsCount
    {
        public long Enqueued { get; set; }
        public long Fetched { get; set; }
    }
}
