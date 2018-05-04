using System;
using System.Collections.Concurrent;
using System.Data;
using System.Threading;
using Hangfire.Logging;
using Npgsql;

namespace Hangfire.PostgreSql.Connectivity
{
    internal sealed class DefaultConnectionProvider : IConnectionProvider
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(DefaultConnectionProvider));

        private readonly string _connectionString;
        private readonly ConcurrentBag<NpgsqlConnection> _connectionQueue;
        private readonly ConcurrentBag<NpgsqlConnection> _connectionPool;

        private int _activeConnections;
        private readonly int _minConnections;
        private readonly int _maxConnections;

        public DefaultConnectionProvider(string connectionString)
        {
            Guard.ThrowIfConnectionStringIsInvalid(connectionString);
            
            _connectionString = connectionString;
            _connectionQueue = new ConcurrentBag<NpgsqlConnection>();
            _connectionPool = new ConcurrentBag<NpgsqlConnection>();

            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(_connectionString);
            _minConnections = connectionStringBuilder.MinPoolSize;
            _maxConnections = connectionStringBuilder.MaxPoolSize;

            if (_minConnections != 0)
            {
                PreInitializePool();
            }
        }

        private void PreInitializePool()
        {
            // simple and stupid impl now. TODO: rework
            for (var i = 0; i < _minConnections; i++)
            {
                using (var connection = AcquireConnection())
                {
                    // do nothing
                }
            }
        }

        internal int ActiveConnections => _activeConnections;

        public ConnectionHolder AcquireConnection()
        {
            var connection = GetFreeConnection();
            return new ConnectionHolder(connection, ReleaseConnection);
        }

        private void ReleaseConnection(ConnectionHolder connectionHolder)
        {
            if (connectionHolder.Disposed) return;
            _connectionQueue.Add(connectionHolder.Connection);
        }

        private NpgsqlConnection GetFreeConnection()
        {
            var spinWait = new SpinWait();
            NpgsqlConnection connection;
            while (!_connectionQueue.TryTake(out connection))
            {
                connection = CreateConnectionIfNeeded();
                if (connection != null) return connection;

                spinWait.SpinOnce();
            }
            return connection;
        }

        private NpgsqlConnection CreateConnectionIfNeeded()
        {
            if (_activeConnections >= _maxConnections) return null;
            Interlocked.Increment(ref _activeConnections);

            if (_activeConnections > _maxConnections)
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
