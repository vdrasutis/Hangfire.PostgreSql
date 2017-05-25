using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Hangfire.Logging;
using Npgsql;

namespace Hangfire.PostgreSql
{
    internal class PostgreSqlConnectionProvider : IPostgreSqlConnectionProvider
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(PostgreSqlConnectionProvider));

        private readonly ConcurrentQueue<NpgsqlConnection> _connectionsQueue;
        private readonly List<NpgsqlConnection> _connections;

        private readonly string _connectionString;
        private readonly PostgreSqlStorageOptions _options;
        private int _connectionsCreated;

        private readonly object _connectionsLock = new object();

        public PostgreSqlConnectionProvider(string connectionString, PostgreSqlStorageOptions options)
        {
            _connectionString = connectionString;
            _options = options;
            _connectionsQueue = new ConcurrentQueue<NpgsqlConnection>();
            _connections = new List<NpgsqlConnection>((int)_options.ConnectionsCount);
        }

        public PostgreSqlConnectionHolder AcquireConnection()
        {
            var connection = GetFreeConnection();
            return new PostgreSqlConnectionHolder(this, connection);
        }

        public void ReleaseConnection(PostgreSqlConnectionHolder connectionHolder)
        {
            if (connectionHolder.Disposed) return;
            _connectionsQueue.Enqueue(connectionHolder.Connection);
        }

        private NpgsqlConnection GetFreeConnection()
        {
            NpgsqlConnection connection;
            while (!_connectionsQueue.TryDequeue(out connection))
            {
                connection = CreateConnectionIfNeeded();
                if (connection != null) return connection;

                Task.Delay(5).Wait();
            }
            return connection;
        }

        private NpgsqlConnection CreateConnectionIfNeeded()
        {
            if (_connectionsCreated >= _options.ConnectionsCount) return null;

            lock (_connectionsLock)
            {
                if (_connectionsCreated >= _options.ConnectionsCount) return null;

                NpgsqlConnection newConnection;
                try
                {
                    newConnection = new NpgsqlConnection(_connectionString);
                    newConnection.Open();
                }
                catch (Exception e)
                {
                    Logger.ErrorFormat("Error while creating connection", e);
                    throw;
                }
                _connectionsCreated++;
                _connections.Add(newConnection);

                return newConnection;
            }
        }

        public void Dispose()
        {
            while (!_connectionsQueue.IsEmpty)
            {
                if (_connectionsQueue.TryDequeue(out var connection))
                {
                    connection.Dispose();
                }
            }
        }
    }
}
