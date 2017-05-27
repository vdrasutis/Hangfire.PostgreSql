using System;

namespace Hangfire.PostgreSql
{
    internal interface IPostgreSqlConnectionProvider : IDisposable
    {
        PostgreSqlConnectionHolder AcquireConnection();
        void ReleaseConnection(PostgreSqlConnectionHolder connectionHolder);
    }
}
