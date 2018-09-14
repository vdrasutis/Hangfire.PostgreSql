using System;
using System.Data;
using Npgsql;

namespace Hangfire.PostgreSql.Connectivity
{
    internal sealed class ConnectionHolder : IDisposable
    {
        private readonly NpgsqlConnection _connection;
        private readonly Action<ConnectionHolder> _connectionDisposer;

        public ConnectionHolder(NpgsqlConnection connection, Action<ConnectionHolder> connectionDisposer)
        {
            _connection = connection;
            _connectionDisposer = connectionDisposer;
        }

        public NpgsqlConnection Connection
        {
            get
            {
                if (Disposed)
                {
                    throw new ObjectDisposedException(nameof(Connection));
                }

                if (_connection.State == ConnectionState.Closed)
                {
                    Disposed = true;
                    throw new ObjectDisposedException(nameof(Connection));
                }

                return _connection;
            }
        }

        public bool Disposed { get; private set; }

        public void Dispose()
        {
            if (Disposed) return;

            _connectionDisposer(this);
            Disposed = true;
        }
    }
}
