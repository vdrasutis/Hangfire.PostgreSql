using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Entities;
using Hangfire.Server;
using Hangfire.Storage;

// ReSharper disable RedundantAnonymousTypePropertyName
namespace Hangfire.PostgreSql
{
    internal class PostgreSqlConnection : JobStorageConnection
    {
        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private readonly IPersistentJobQueue _queue;
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlConnection(
            IPostgreSqlConnectionProvider connectionProvider,
            IPersistentJobQueue queue,
            PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(queue, nameof(queue));
            Guard.ThrowIfNull(options, nameof(options));

            _connectionProvider = connectionProvider;
            _queue = queue;
            _options = options;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
            => new PostgreSqlWriteOnlyTransaction(_connectionProvider, _queue, _options);

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
            => new PostgreSqlDistributedLock(
                "hangfire:" + resource,
                timeout,
                _connectionProvider,
                _options);

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0) throw new ArgumentNullException(nameof(queues));
            return _queue.Dequeue(queues, cancellationToken);
        }

        public override string CreateExpiredJob(
            Job job,
            IDictionary<string, string> parameters,
            DateTime createdAt,
            TimeSpan expireIn)
        {
            Guard.ThrowIfNull(job, nameof(job));
            Guard.ThrowIfNull(parameters, nameof(parameters));

            var createJobSql = $@"
INSERT INTO ""{_options.SchemaName}"".job (invocationdata, arguments, createdat, expireat)
VALUES (@invocationData, @arguments, @createdAt, @expireAt) 
RETURNING id;
";
            var invocationData = InvocationData.Serialize(job);

            int jobId;
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var connection = connectionHolder.Connection;
                jobId = connection.Query<int>(
                    createJobSql,
                    new
                    {
                        invocationData = JobHelper.ToJson(invocationData),
                        arguments = invocationData.Arguments,
                        createdAt = createdAt,
                        expireAt = createdAt.Add(expireIn)
                    }).Single();
            }

            if (parameters.Count > 0)
            {
                var parameterArray = new object[parameters.Count];
                var parameterIndex = 0;
                foreach (var parameter in parameters)
                {
                    parameterArray[parameterIndex++] = new
                    {
                        jobId = jobId,
                        name = parameter.Key,
                        value = parameter.Value
                    };
                }

                var insertParameterSql = $@"
INSERT INTO ""{_options.SchemaName}"".jobparameter (jobid, name, value)
VALUES (@jobId, @name, @value);
";
                using (var connectionHolder = _connectionProvider.AcquireConnection())
                {
                    connectionHolder.Connection.Execute(insertParameterSql, parameterArray);
                }
            }
            return jobId.ToString(CultureInfo.InvariantCulture);
        }

        public override JobData GetJobData(string id)
        {
            Guard.ThrowIfNull(id, nameof(id));

            var sql = $@"
SELECT ""invocationdata"" ""invocationData"", ""statename"" ""stateName"", ""arguments"", ""createdat"" ""createdAt"" 
FROM ""{_options.SchemaName}"".""job"" 
WHERE ""id"" = @id;
";

            SqlJob jobData;
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                jobData = connectionHolder.Connection
                    .Query<SqlJob>(sql, new { id = Convert.ToInt32(id, CultureInfo.InvariantCulture) })
                    .SingleOrDefault();
            }

            if (jobData == null) return null;

            // TODO: conversion exception could be thrown.
            var invocationData = JobHelper.FromJson<InvocationData>(jobData.InvocationData);
            invocationData.Arguments = jobData.Arguments;

            Job job = null;
            JobLoadException loadException = null;

            try
            {
                job = invocationData.Deserialize();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                State = jobData.StateName,
                CreatedAt = jobData.CreatedAt,
                LoadException = loadException
            };
        }

        public override StateData GetStateData(string jobId)
        {
            Guard.ThrowIfNull(jobId, nameof(jobId));

            var query = $@"
SELECT s.name ""Name"", s.reason ""Reason"", s.data ""Data""
FROM ""{_options.SchemaName}"".state s
INNER JOIN ""{_options.SchemaName}"".job j on j.stateid = s.id
WHERE j.id = @jobId;
";

            SqlState sqlState;
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                sqlState = connectionHolder.Connection
                    .Query<SqlState>(query, new { jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) })
                    .SingleOrDefault();
            }

            if (sqlState == null) return null;

            return new StateData
            {
                Name = sqlState.Name,
                Reason = sqlState.Reason,
                Data = JobHelper.FromJson<Dictionary<string, string>>(sqlState.Data)
            };
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            Guard.ThrowIfNull(id, nameof(id));
            Guard.ThrowIfNull(name, nameof(name));

            var sql = @"
WITH ""inputvalues"" AS (
	SELECT @jobid ""jobid"", @name ""name"", @value ""value""
), ""updatedrows"" AS ( 
	UPDATE """ + _options.SchemaName + @""".""jobparameter"" ""updatetarget""
	SET ""value"" = ""inputvalues"".""value""
	FROM ""inputvalues""
	WHERE ""updatetarget"".""jobid"" = ""inputvalues"".""jobid""
	AND ""updatetarget"".""name"" = ""inputvalues"".""name""
	RETURNING ""updatetarget"".""jobid"", ""updatetarget"".""name""
)
INSERT INTO """ + _options.SchemaName + @""".""jobparameter""(""jobid"", ""name"", ""value"")
SELECT ""jobid"", ""name"", ""value"" 
FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
	SELECT 1 
	FROM ""updatedrows"" 
	WHERE ""updatedrows"".""jobid"" = ""insertvalues"".""jobid"" 
	AND ""updatedrows"".""name"" = ""insertvalues"".""name""
);";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var parameters = new { jobId = Convert.ToInt32(id, CultureInfo.InvariantCulture), name, value };
                connectionHolder.Connection.Execute(sql, parameters);
            }
        }

        public override string GetJobParameter(string id, string name)
        {
            Guard.ThrowIfNull(id, nameof(id));
            Guard.ThrowIfNull(name, nameof(name));

            var query = $@"SELECT value FROM ""{_options.SchemaName}"".jobparameter WHERE jobid = @id AND name = @name;";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var parameters = new { id = Convert.ToInt32(id, CultureInfo.InvariantCulture), name = name };
                return connectionHolder.Connection.Query<string>(query, parameters).SingleOrDefault();
            }
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var query = $@"SELECT ""value"" FROM ""{_options.SchemaName}"".""set"" WHERE ""key"" = @key;";


            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var result = connectionHolder.Connection.Query<string>(query, new { key = key });
                return new HashSet<string>(result);
            }
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (toScore < fromScore)
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");


            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<string>(
                        @"
SELECT ""value"" 
FROM """ + _options.SchemaName + @""".""set"" 
WHERE ""key"" = @key 
AND ""score"" BETWEEN @from AND @to 
ORDER BY ""score"" LIMIT 1;
",
                        new { key, from = fromScore, to = toScore })
                    .SingleOrDefault();
            }
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            var sql = @"
WITH ""inputvalues"" AS (
	SELECT @key ""key"", @field ""field"", @value ""value""
), ""updatedrows"" AS ( 
	UPDATE """ + _options.SchemaName + @""".""hash"" ""updatetarget""
	SET ""value"" = ""inputvalues"".""value""
	FROM ""inputvalues""
	WHERE ""updatetarget"".""key"" = ""inputvalues"".""key""
	AND ""updatetarget"".""field"" = ""inputvalues"".""field""
	RETURNING ""updatetarget"".""key"", ""updatetarget"".""field""
)
INSERT INTO """ + _options.SchemaName + @""".""hash""(""key"", ""field"", ""value"")
SELECT ""key"", ""field"", ""value"" FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
	SELECT 1 
	FROM ""updatedrows"" 
	WHERE ""updatedrows"".""key"" = ""insertvalues"".""key"" 
	AND ""updatedrows"".""field"" = ""insertvalues"".""field""
);
";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            using (var transaction = connectionHolder.Connection.BeginTransaction(IsolationLevel.Serializable))
            {
                foreach (var keyValuePair in keyValuePairs)
                {
                    connectionHolder.Connection.Execute(sql, new { key = key, field = keyValuePair.Key, value = keyValuePair.Value },
                        transaction);
                }
                transaction.Commit();
            }
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var result = connectionHolder.Connection.Query<SqlHash>(
                        $@"SELECT ""field"" ""Field"", ""value"" ""Value"" 
					FROM ""{_options.SchemaName}"".""hash"" 
					WHERE ""key"" = @key;
					",
                        new { key = key })
                    .ToDictionary(x => x.Field, x => x.Value);

                return result.Count != 0 ? result : null;
            }
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            if (context == null) throw new ArgumentNullException(nameof(context));

            var data = new ServerData
            {
                WorkerCount = context.WorkerCount,
                Queues = context.Queues,
                StartedAt = DateTime.UtcNow,
            };

            var sql = @"
WITH ""inputvalues"" AS (
	SELECT @id ""id"", @data ""data"", NOW() AT TIME ZONE 'UTC' ""lastheartbeat""
), ""updatedrows"" AS ( 
	UPDATE """ + _options.SchemaName + @""".""server"" ""updatetarget""
	SET ""data"" = ""inputvalues"".""data"", ""lastheartbeat"" = ""inputvalues"".""lastheartbeat""
	FROM ""inputvalues""
	WHERE ""updatetarget"".""id"" = ""inputvalues"".""id""
	RETURNING ""updatetarget"".""id""
)
INSERT INTO """ + _options.SchemaName + @""".""server""(""id"", ""data"", ""lastheartbeat"")
SELECT ""id"", ""data"", ""lastheartbeat"" FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
	SELECT 1 
	FROM ""updatedrows"" 
	WHERE ""updatedrows"".""id"" = ""insertvalues"".""id"" 
);
";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                connectionHolder.Connection.Execute(sql,
                    new { id = serverId, data = JobHelper.ToJson(data) });
            }
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));


            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                connectionHolder.Connection.Execute(
                    $@"DELETE FROM ""{_options.SchemaName}"".""server"" WHERE ""id"" = @id;",
                    new { id = serverId });
            }
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            var query =
                $@"UPDATE ""{_options.SchemaName}"".""server"" 
				SET ""lastheartbeat"" = NOW() AT TIME ZONE 'UTC' 
				WHERE ""id"" = @id;";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                connectionHolder.Connection.Execute(query, new { id = serverId });
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            }

            var query =
                $@"DELETE FROM ""{_options.SchemaName}"".""server"" 
				WHERE ""lastheartbeat"" < (NOW() AT TIME ZONE 'UTC' - INTERVAL '{
                        (
                            long)timeOut.TotalMilliseconds
                    } MILLISECONDS');";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Execute(query);
            }
        }

        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var query = $@"select count(""key"") from ""{_options.SchemaName}"".""set"" where ""key"" = @key";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<long>(query, new { key }).First();
            }
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var query =
                $@"select ""value"" from ""{_options.SchemaName}"".""list"" where ""key"" = @key order by ""id"" desc";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<string>(query, new { key }).ToList();
            }
        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var query =
                $@"select sum(s.""Value"") from (select sum(""value"") as ""Value"" from ""{
                        _options.SchemaName
                    }"".""counter"" where ""key"" = @key) s";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<long?>(query, new { key }).SingleOrDefault() ?? 0;
            }
        }

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var query = $@"select count(""id"") from ""{_options.SchemaName}"".""list"" where ""key"" = @key";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<long>(query, new { key }).SingleOrDefault();
            }
        }

        public override TimeSpan GetListTtl(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            var query = $@"select min(""expireat"") from ""{_options.SchemaName}"".""list"" where ""key"" = @key";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var result = connectionHolder.Connection.Query<DateTime?>(query, new { key }).Single();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            }
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            Guard.ThrowIfNull(key, nameof(key));

            var query = $@"select ""value"" from (
					select ""value"", row_number() over (order by ""id"" desc) as row_num 
					from ""{_options.SchemaName}"".""list""
					where ""key"" = @key 
				) as s where s.row_num between @startingFrom and @endingAt";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection
                    .Query<string>(query, new { key, startingFrom = startingFrom + 1, endingAt = endingAt + 1 })
                    .ToList();
            }
        }

        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var query = $@"select count(""id"") from ""{_options.SchemaName}"".""hash"" where ""key"" = @key";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<long>(query, new { key }).SingleOrDefault();
            }
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var query = $@"select min(""expireat"") from ""{_options.SchemaName}"".""hash"" where ""key"" = @key";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var result = connectionHolder.Connection.Query<DateTime?>(query, new { key }).Single();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            }
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            Guard.ThrowIfNull(key, nameof(key));

            var query = $@"select ""value"" from (
					select ""value"", row_number() over (order by ""id"" ASC) as row_num 
					from ""{_options.SchemaName}"".""set""
					where ""key"" = @key 
				) as s where s.row_num between @startingFrom and @endingAt";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<string>(query,
                        new { key, startingFrom = startingFrom + 1, endingAt = endingAt + 1 })
                    .ToList();
            }
        }

        public override TimeSpan GetSetTtl(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            var query = $@"SELECT MIN(expireat) FROM ""{_options.SchemaName}"".set WHERE key = @key";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var result = connectionHolder.Connection.Query<DateTime?>(query, new { key }).SingleOrDefault();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            }
        }

        public override string GetValueFromHash(string key, string name)
        {
            Guard.ThrowIfNull(key, nameof(key));
            Guard.ThrowIfNull(name, nameof(name));

            var query =
                $@"select ""value"" from ""{
                        _options.SchemaName
                    }"".""hash"" where ""key"" = @key and ""field"" = @field";


            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<string>(query, new { key, field = name }).SingleOrDefault();
            }
        }
    }
}
