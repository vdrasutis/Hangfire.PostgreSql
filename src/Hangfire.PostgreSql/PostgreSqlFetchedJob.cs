// This file is part of Hangfire.PostgreSql.
// Copyright © 2014 Frank Hommers <http://hmm.rs/Hangfire.PostgreSql>.
// 
// Hangfire.PostgreSql is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.PostgreSql  is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.PostgreSql. If not, see <http://www.gnu.org/licenses/>.
//
// This work is based on the work of Sergey Odinokov, author of 
// Hangfire. <http://hangfire.io/>
//   
//    Special thanks goes to him.

using System;
using System.Data;
using Dapper;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
#if (NETCORE1 || NETCORE50 || NETSTANDARD1_5 || NETSTANDARD1_6)
    public
#else
	internal
#endif
        class PostgreSqlFetchedJob : IFetchedJob
    {
        private readonly IDbConnection _connection;
        private readonly PostgreSqlStorageOptions _options;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;

        public PostgreSqlFetchedJob(
            IDbConnection connection,
            PostgreSqlStorageOptions options,
            int id,
            string jobId,
            string queue)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _options = options ?? throw new ArgumentNullException(nameof(options));

            Id = id;
            JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
            Queue = queue ?? throw new ArgumentNullException(nameof(queue));
        }

        public int Id { get; }
        public string JobId { get; }
        public string Queue { get; }

        public void RemoveFromQueue()
        {
            var sql = $@"
DELETE FROM ""{_options.SchemaName}"".""jobqueue"" 
WHERE ""id"" = @id;
";
            _connection.Execute(sql, new {id = Id});
            _removedFromQueue = true;
        }

        public void Requeue()
        {
            var sql = $@"
UPDATE ""{_options.SchemaName}"".""jobqueue"" 
SET ""fetchedat"" = NULL 
WHERE ""id"" = @id;
";
            _connection.Execute(sql, new {id = Id});
            _requeued = true;
        }

        public void Dispose()
        {
            if (_disposed) return;

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _disposed = true;
        }
    }
}
