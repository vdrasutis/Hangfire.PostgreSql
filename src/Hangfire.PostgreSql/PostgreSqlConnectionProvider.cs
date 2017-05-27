using System;
using System.Collections.Concurrent;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Logging;
using Npgsql;

namespace Hangfire.PostgreSql
{
    internal class PostgreSqlConnectionProvider : IPostgreSqlConnectionProvider
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(PostgreSqlConnectionProvider));

        private readonly string _connectionString;
        private readonly PostgreSqlStorageOptions _options;
        private int _activeConnections;
        private readonly ConcurrentQueue<NpgsqlConnection> _connectionQueue;
        private readonly ConcurrentBag<NpgsqlConnection> _connectionPool;

        public PostgreSqlConnectionProvider(string connectionString, PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfConnectionStringIsInvalid(connectionString);
            Guard.ThrowIfNull(options, nameof(options));

            _connectionString = connectionString;
            _options = options;
            _connectionQueue = new ConcurrentQueue<NpgsqlConnection>();
            _connectionPool = new ConcurrentBag<NpgsqlConnection>();
        }

        internal int ActiveConnections => _activeConnections;

        public PostgreSqlConnectionHolder AcquireConnection()
        {
            var connection = GetFreeConnection();
            return new PostgreSqlConnectionHolder(this, connection);
        }

        public void ReleaseConnection(PostgreSqlConnectionHolder connectionHolder)
        {
            if (connectionHolder.Disposed) return;
            _connectionQueue.Enqueue(connectionHolder.Connection);
        }

        private NpgsqlConnection GetFreeConnection()
        {
            NpgsqlConnection connection;
            while (!_connectionQueue.TryDequeue(out connection))
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
            Interlocked.Increment(ref _activeConnections);

            if (_activeConnections > _options.ConnectionsCount)
            {
                Interlocked.Decrement(ref _activeConnections);
                return null;
            }

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
            newConnection.StateChange += ConnectionStateChanged;
            _connectionPool.Add(newConnection);

            return newConnection;
        }

        private void ConnectionStateChanged(object sender, StateChangeEventArgs stateChangeEventArgs)
        {
            if (stateChangeEventArgs.CurrentState != ConnectionState.Open)
            {
                Logger.Info("Connection was removed from pool");
                Interlocked.Decrement(ref _activeConnections);
            }
        }

        public void Dispose()
        {
            while (_connectionPool.TryTake(out var connection))
            {
                connection.Dispose();
            }
        }
    }
}
