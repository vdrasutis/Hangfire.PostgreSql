using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Logging;
using Npgsql;

namespace Hangfire.PostgreSql
{
    internal class PostgreSqlConnectionHolder : IDisposable
    {
        private readonly ConcurrentQueue<NpgsqlConnection> _connections;
        private readonly NpgsqlConnection _connection;

        public PostgreSqlConnectionHolder(ConcurrentQueue<NpgsqlConnection> connections, NpgsqlConnection connection)
        {
            _connections = connections;
            _connection = connection;
        }

        public NpgsqlConnection Connection
        {
            get
            {
                if (Disposed) throw new ObjectDisposedException(nameof(Connection));
                return _connection;
            }
        }

        public bool Disposed { get; set; }

        public void Dispose()
        {
            if (!Disposed)
            {
                _connections.Enqueue(_connection);
                Disposed = true;
            }
        }
    }

    internal interface IPostgreSqlConnectionProvider : IDisposable
    {
        PostgreSqlConnectionHolder AcquireConnection();
    }

    internal class PostgreSqlConnectionProvider : IPostgreSqlConnectionProvider
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(PostgreSqlConnectionProvider));

        private readonly ConcurrentQueue<NpgsqlConnection> _connections;
        private readonly string _connectionString;
        private readonly PostgreSqlStorageOptions _options;
        private int _connectionsCreated;

        public PostgreSqlConnectionProvider(string connectionString, PostgreSqlStorageOptions options)
        {
            _connectionString = connectionString;
            _options = options;
            _connections = new ConcurrentQueue<NpgsqlConnection>();
        }

        public PostgreSqlConnectionHolder AcquireConnection()
        {
            var connection = GetFreeConnection();
            return new PostgreSqlConnectionHolder(_connections, connection);
        }

        private NpgsqlConnection GetFreeConnection()
        {
            NpgsqlConnection connection;
            while (!_connections.TryDequeue(out connection))
            {
                CreateConnectionIfNeeded();
                Task.Delay(5).Wait();
            }
            return connection;
        }

        private void CreateConnectionIfNeeded()
        {
            if (_connectionsCreated >= _options.ConnectionsCount) return;

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

            Interlocked.Increment(ref _connectionsCreated);
            _connections.Enqueue(newConnection);
        }

        public void Dispose()
        {
            while (!_connections.IsEmpty)
            {
                if (_connections.TryDequeue(out var connection))
                {
                    connection.Dispose();
                }
            }
        }
    }

    internal static class PostgreSqlConnectionProviderUtils
    {
        public static void Execute(this IPostgreSqlConnectionProvider provider, Action<NpgsqlConnection> action)
        {
            using (var connectionHolder = provider.AcquireConnection())
            {
                action(connectionHolder.Connection);
            }
        }

        public static T Execute<T>(this IPostgreSqlConnectionProvider provider, Func<NpgsqlConnection, T> action)
        {
            using (var connectionHolder = provider.AcquireConnection())
            {
                return action(connectionHolder.Connection);
            }
        }
    }
}
