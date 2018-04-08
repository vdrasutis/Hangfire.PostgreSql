using System;
using System.Data;
using Npgsql;

namespace Hangfire.PostgreSql
{
    internal sealed class PostgreSqlConnectionHolder : IDisposable
    {
        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private readonly NpgsqlConnection _connection;

        public PostgreSqlConnectionHolder(IPostgreSqlConnectionProvider connectionProvider, NpgsqlConnection connection)
        {
            _connectionProvider = connectionProvider;
            _connection = connection;
            GC.SuppressFinalize(this);
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

            _connectionProvider.ReleaseConnection(this);
            Disposed = true;
        }
    }
}
