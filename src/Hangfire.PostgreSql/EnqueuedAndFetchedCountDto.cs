namespace Hangfire.PostgreSql
{
    public class EnqueuedAndFetchedCountDto
    {
        public long? EnqueuedCount { get; set; }
        public long? FetchedCount { get; set; }
    }
}
