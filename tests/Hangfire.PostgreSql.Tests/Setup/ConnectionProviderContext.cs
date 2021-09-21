using System;
using Hangfire.PostgreSql.Connectivity;

namespace Hangfire.PostgreSql.Tests.Setup
{
    public class NpgsqlConnectionProviderContext : IDisposable
    {
        internal NpgsqlConnectionProvider ConnectionProvider { get; }

        public NpgsqlConnectionProviderContext()
        {
            ConnectionProvider = ConnectivityUtilities.CreateNpgsqlConnectionProvider();
        }

        public void Dispose() => ConnectionProvider.Dispose();
    }
}
