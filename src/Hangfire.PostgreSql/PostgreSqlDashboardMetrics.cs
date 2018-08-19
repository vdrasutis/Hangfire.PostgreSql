using System;
using Dapper;
using Hangfire.Annotations;
using Hangfire.Dashboard;
using Npgsql;

namespace Hangfire.PostgreSql
{
    public static class PostgreSqlDashboardMetrics
    {
        [PublicAPI]
        public static readonly DashboardMetric MaxConnections = new DashboardMetric(
            "pg:connections:max",
            "Max Connections",
            page => GetMetricByQuery<long>(page, @"SHOW max_connections;"));

        [PublicAPI]
        public static readonly DashboardMetric ActiveConnections = new DashboardMetric(
            "pg:connections:active",
            "Active Connections",
            page => GetMetricByQuery<long>(page, @"SELECT numbackends from pg_stat_database WHERE datname = current_database();"));

        [PublicAPI]
        public static readonly DashboardMetric PostgreSqlLocksCount = new DashboardMetric(
            "pg:locks:count",
            "PostgreSql Locks",
            page => GetMetricByQuery<long>(page, @"SELECT COUNT(*) FROM pg_locks;"));

        [PublicAPI]
        public static readonly DashboardMetric DistributedLocksCount = new DashboardMetric(
            "app:locks:count",
            "Distributed Locks",
            page => GetMetricByQuery<long>(page, @"SELECT COUNT(*) FROM lock;"));

        [PublicAPI]
        public static readonly DashboardMetric PostgreSqlServerVersion = new DashboardMetric(
            "pg:version",
            "PostgreSql Version",
            page => Execute(page, x => new Metric(x.PostgreSqlVersion.ToString()) { Style = MetricStyle.Info }, UndefinedMetric));

        [PublicAPI]
        public static readonly DashboardMetric CacheHitsPerRead = new DashboardMetric(
            "pg:cache:hitratio",
            "Cache Hits Per Read",
            page => GetMetricByQuery<long>(page, @"SELECT ROUND(SUM(blks_hit) / SUM(blks_read)) FROM pg_stat_database;"));

        [PublicAPI]
        public static readonly DashboardMetric ConnectionUsageRatio = new DashboardMetric(
            "pg:connections:ratio",
            "Connections usage",
            page => Execute(page, connection =>
            {
                var max = connection.ExecuteScalar<long>(@"SHOW max_connections;");
                var current = connection.ExecuteScalar<long>(@"SELECT numbackends from pg_stat_database WHERE datname = current_database();");

                var ratio = current * 100 / max;
                var ratioString = ratio + "%";
                if (ratio < 50)
                {
                    return new Metric(ratioString) { Style = MetricStyle.Success };
                }
                else if (ratio < 90)
                {
                    return new Metric(ratioString) { Style = MetricStyle.Warning };
                }
                else
                {
                    return new Metric(ratioString) { Style = MetricStyle.Danger, Highlighted = true };
                }
            }, UndefinedMetric)
        );

        private static readonly Metric UndefinedMetric = new Metric("N/A") { Highlighted = true, Style = MetricStyle.Danger };

        private static Metric GetMetricByQuery<T>(RazorPage page, string query)
        {
            return Execute(page, connection =>
            {
                var metric = connection.ExecuteScalar<T>(query);
                return new Metric(metric.ToString());
            }, UndefinedMetric);
        }

        private static T Execute<T>(RazorPage page, Func<NpgsqlConnection, T> func, T fallbackValue)
        {
            if (page.Storage is PostgreSqlStorage storage)
            {
                using (var connectionHolder = storage.ConnectionProvider.AcquireConnection())
                {
                    return func(connectionHolder.Connection);
                }
            }
            else
            {
                return fallbackValue;
            }
        }
    }
}
