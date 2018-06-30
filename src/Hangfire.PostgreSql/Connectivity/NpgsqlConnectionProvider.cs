using Npgsql;

namespace Hangfire.PostgreSql.Connectivity
{
    internal sealed class NpgsqlConnectionProvider : IConnectionProvider
    {
        private readonly string _connectionString;

        public NpgsqlConnectionProvider(string connectionString)
        {
            _connectionString = connectionString;
        }

        public ConnectionHolder AcquireConnection()
        {
            var connection = new NpgsqlConnection(_connectionString);
            connection.Open();
            return new ConnectionHolder(connection, holder => holder.Connection.Dispose());
        }

        public void Dispose()
        {
            // Npgsql will handle disposing of internal pool by itself
        }
    }
}
