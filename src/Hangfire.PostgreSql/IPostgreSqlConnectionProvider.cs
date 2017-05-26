using System;

namespace Hangfire.PostgreSql
{
    internal interface IPostgreSqlConnectionProvider : IDisposable
    {
        int ActiveConnections { get; }
        PostgreSqlConnectionHolder AcquireConnection();
        void ReleaseConnection(PostgreSqlConnectionHolder connectionHolder);
    }
}
