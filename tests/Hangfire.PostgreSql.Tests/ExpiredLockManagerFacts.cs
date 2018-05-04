using System;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Maintenance;
using Hangfire.PostgreSql.Tests.Utils;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class ExpiredLockManagerFacts
    {
        [Fact, CleanDatabase]
        public void Execute_RemovesExpiredLocks()
        {
            // Arrange
            var timeout = TimeSpan.FromSeconds(7);

            UseConnection((provider, connection) =>
            {
                connection.Execute(@"INSERT INTO lock(resource, acquired) VALUES ('hello', current_timestamp at time zone 'UTC' - @timeout)", new { timeout });

                // Act
                var expiredLocksManager = new ExpiredLocksManager(provider, timeout);
                expiredLocksManager.Execute(CancellationToken.None);

                // Assert
                var locksCount = connection.ExecuteScalar<int>(@"SELECT COUNT(*) FROM lock WHERE resource = 'hello'");
                Assert.Equal(0, locksCount);
            });
        }

        private void UseConnection(Action<IConnectionProvider, NpgsqlConnection> action)
        {
            var provider = ConnectionUtils.GetConnectionProvider();
            using (var connection = provider.AcquireConnection())
            {
                action(provider, connection.Connection);
            }
        }
    }
}
