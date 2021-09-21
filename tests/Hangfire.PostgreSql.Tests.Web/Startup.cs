using System;
using Hangfire.Console;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Hangfire.PostgreSql.Tests.Web
{
    public class Startup
    {
        private const string DefaultConnectionString =
            @"Server=localhost;Port=5432;Database=hangfire_tests;User Id=postgres;Password=password;Search Path=hangfire";

        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {
            var connectionString = Environment.GetEnvironmentVariable("Hangfire_PostgreSql_ConnectionString") ??
                                   DefaultConnectionString;
            services.AddMvc();
            services.AddHangfire(configuration =>
            {
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.ActiveConnections);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.MaxConnections);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.ConnectionUsageRatio);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.CacheHitsPerRead);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.DistributedLocksCount);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.PostgreSqlLocksCount);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.PostgreSqlServerVersion);
                configuration.UsePostgreSqlStorage(connectionString, new PostgreSqlStorageOptions
                {
                    DistributedLockTimeout = TimeSpan.FromSeconds(30)
                });
                configuration.UseConsole();
            });

            services.AddHangfireServer(options =>
            {
                options.ServerCheckInterval = TimeSpan.FromSeconds(15);
                options.HeartbeatInterval = TimeSpan.FromSeconds(5);
                options.ServerTimeout = TimeSpan.FromSeconds(15);
                options.ServerName = "Hangfire Test Server";
                options.WorkerCount = 50;
                options.Queues = new[] { "queue2", "queue1", "default" };
            });
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            app.UseDeveloperExceptionPage();
            app.UseHangfireDashboard("", new DashboardOptions { StatsPollingInterval = 1000 });
            
            RecurringJob.AddOrUpdate(() => TestSuite.Alloc(), Cron.Yearly, TimeZoneInfo.Utc);
            RecurringJob.AddOrUpdate(() => TestSuite.CpuKill(25), Cron.Yearly, TimeZoneInfo.Utc);
            RecurringJob.AddOrUpdate(() => TestSuite.ContinuationTest(), Cron.Yearly, TimeZoneInfo.Utc);
            RecurringJob.AddOrUpdate(() => TestSuite.TaskBurst(), Cron.Yearly, TimeZoneInfo.Utc);
            //RecurringJob.AddOrUpdate(() => DistributedLockTest.Ctor_ActuallyGrantsExclusiveLock(), Cron.Yearly, TimeZoneInfo.Utc);
            //RecurringJob.AddOrUpdate(() => DistributedLockTest.Perf_AcquiringLock_DifferentResources(), Cron.Yearly,
            //    TimeZoneInfo.Utc);
            //RecurringJob.AddOrUpdate(() => DistributedLockTest.Perf_AcquiringLock_SameResource(), Cron.Yearly, TimeZoneInfo.Utc);
            //RecurringJob.AddOrUpdate(() => TestSuite.ContinuationPartC4(), Cron.Yearly, queue: "queue-does-not-exist");
        }
    }
}
