
# Hangfire.PostgreSql
[![Build status](https://ci.appveyor.com/api/projects/status/n05446uxa1f5sjw3?svg=true)](https://ci.appveyor.com/project/ahydrax/hangfire-postgresql)
[![Code quality](https://sonarcloud.io/api/project_badges/measure?project=Hangfire.PostgreSql.ahydrax&metric=alert_status)](https://sonarcloud.io/dashboard?id=Hangfire.PostgreSql.ahydrax)
[![NuGet](https://img.shields.io/nuget/v/Hangfire.PostgreSql.ahydrax.svg)](https://www.nuget.org/packages/Hangfire.PostgreSql.ahydrax/)
[![GitHub license](https://img.shields.io/badge/license-LGPL-blue.svg?style=flat)](https://raw.githubusercontent.com/ahydrax/Hangfire.PostgreSql/master/COPYING)

This is a plugin for Hangfire to enable PostgreSql as a storage system.
Read about hangfire here: https://github.com/HangfireIO/Hangfire#hangfire-
and here: http://hangfire.io/

## Requirements
* `>= .NET 4.5.2` or `>= .NET Standard 1.6`
* `>= PostgreSql 9.6`

## Usage
Install Hangfire, see https://github.com/HangfireIO/Hangfire#installation

Download source files and build your own binaries or just use nuget package.

```csharp
app.UseHangfireServer(new BackgroundJobServerOptions(), 
  new PostgreSqlStorage(connectionString));
app.UseHangfireDashboard();
```
`connectionString` **must be** supplied with `Search Path = <schema name>` parameter.

## Additional metrics for Hangfire.Dashboard
![dashboard](content/dashboard.png)
Metrics can be added in two different ways:

If you want to use recommended settings then do:

```csharp
GlobalConfiguration.Configuration.UsePostgreSqlMetrics();
```

Or setup manually according to your needings:
```csharp
GlobalConfiguration.Configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.MaxConnections);
GlobalConfiguration.Configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.ActiveConnections);
GlobalConfiguration.Configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.DistributedLocksCount);
GlobalConfiguration.Configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.PostgreSqlLocksCount);
GlobalConfiguration.Configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.CacheHitsPerRead);
GlobalConfiguration.Configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.PostgreSqlServerVersion);
GlobalConfiguration.Configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.ConnectionUsageRatio);
```

## Related links

* [Hangfire.Core](https://github.com/HangfireIO/Hangfire)
* [Hangfire.Postgres original project](https://github.com/frankhommers/Hangfire.PostgreSql)

## License

Copyright © 2014-2018 Frank Hommers, Burhan Irmikci (barhun), Zachary Sims(zsims), kgamecarter, Stafford Williams (staff0rd), briangweber, Viktor Svyatokha (ahydrax), Christopher Dresel (Dresel), Vytautas Kasparavičius (vytautask).

Hangfire.PostgreSql is an Open Source project licensed under the terms of the LGPLv3 license. Please see http://www.gnu.org/licenses/lgpl-3.0.html for license text or COPYING.LESSER file distributed with the source code.

This work is based on the work of Sergey Odinokov, author of Hangfire. <http://hangfire.io/>
