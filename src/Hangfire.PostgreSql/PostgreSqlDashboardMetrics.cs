using Dapper;
using Hangfire.Dashboard;

namespace Hangfire.PostgreSql
{
    public static class PostgreSqlDashboardMetrics
    {
        public static readonly DashboardMetric MaxConnections = new DashboardMetric(
            "pg:connections:max",
            "Max Connections",
            page => GetMetricByQuery(page, @"SHOW max_connections;"));

        public static readonly DashboardMetric ActiveConnections = new DashboardMetric(
            "pg:connections:active",
            "Active Connections",
            page => GetMetricByQuery(page, @"SELECT numbackends from pg_stat_database WHERE datname = current_database();"));

        public static readonly DashboardMetric PostgreSqlLocksCount = new DashboardMetric(
            "pg:locks:count",
            "PostgreSql Locks Count",
            page => GetMetricByQuery(page, @"SELECT COUNT(*) FROM pg_locks;"));

        public static readonly DashboardMetric DistributedLocksCount = new DashboardMetric(
            "app:locks:count",
            "Distributed Locks Count",
            page => GetMetricByQuery(page, @"SELECT COUNT(*) FROM lock;"));

        public static readonly DashboardMetric PostgreSqlServerVersion = new DashboardMetric(
            "pg:version",
            "PostgreSql Version",
            page => GetMetricByQuery(page, @"SHOW server_version;"));

        public static readonly DashboardMetric CacheHitsPerRead = new DashboardMetric(
            "pg:cache:hitratio",
            "Cache Hits Per Read",
            page => GetMetricByQuery(page, @"SELECT ROUND(SUM(blks_hit) / SUM(blks_read)) FROM pg_stat_database;"));

        private static Metric GetMetricByQuery(RazorPage page, string query)
        {
            var storage = page.Storage as PostgreSqlStorage;
            if (storage == null) return new Metric("???");

            using (var connectionHolder = storage.ConnectionProvider.AcquireConnection())
            {
                var searchPathQuery = $"SET search_path = '{storage.Options.SchemaName}';";
                var serverVersion = connectionHolder.Connection.ExecuteScalar(searchPathQuery + query);
                return new Metric(serverVersion?.ToString() ?? "???");
            }
        }
    }
}
