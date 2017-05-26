using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
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
        private int _activeConnections;

        private readonly object _connectionsLock = new object();

        public PostgreSqlConnectionProvider(string connectionString, PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfConnectionStringIsInvalid(connectionString);
            Guard.ThrowIfNull(options, nameof(options));

            _connectionString = connectionString;
            _options = options;
            _connectionsQueue = new ConcurrentQueue<NpgsqlConnection>();
            _connections = new List<NpgsqlConnection>((int)_options.ConnectionsCount);
        }

        public int ActiveConnections => _activeConnections;

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
            if (_activeConnections >= _options.ConnectionsCount) return null;

            lock (_connectionsLock)
            {
                if (_activeConnections >= _options.ConnectionsCount) return null;

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
                _activeConnections++;
                _connections.Add(newConnection);
                newConnection.StateChange += ConnectionStateChanged;

                return newConnection;
            }
        }

        private void ConnectionStateChanged(object sender, StateChangeEventArgs stateChangeEventArgs)
        {
            if (stateChangeEventArgs.CurrentState != ConnectionState.Open)
            {
                lock (_connectionsLock)
                {
                    _connections.Remove(sender as NpgsqlConnection);
                    _activeConnections--;
                }
                Logger.Info("Connection was removed from pool");
            }
        }

        public void Dispose()
        {
            lock (_connectionsLock)
            {
                foreach (var connection in new List<NpgsqlConnection>(_connections))
                {
                    connection.Dispose();
                }
            }
        }
    }
}
