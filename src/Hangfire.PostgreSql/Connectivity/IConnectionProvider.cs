using System;

namespace Hangfire.PostgreSql.Connectivity
{
    internal interface IConnectionProvider : IDisposable
    {
        ConnectionHolder AcquireConnection();
    }
}
