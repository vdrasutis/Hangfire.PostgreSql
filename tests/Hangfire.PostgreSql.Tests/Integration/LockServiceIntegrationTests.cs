using System;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Locking;
using Hangfire.PostgreSql.Tests.Setup;
using Hangfire.Server;
using Npgsql;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.PostgreSql.Tests.Integration
{
    public class LockServiceIntegrationTests : StorageContextBasedTests<LockServiceIntegrationTests>
    {
        [Fact]
        public void Execute_RespectsActiveLocks()
        {
            // Arrange
            using var connection = ConnectionProvider.AcquireConnection();
            connection.Execute(@"
insert into server(id, lastheartbeat, lockacquirerid) 
values ('remote_instance', (now() - interval '1 day') at time zone 'UTC', 'instance_lock_id_1'),
       ('this_instance', now() at time zone 'UTC', @acquirerId)",
                new { acquirerId = LockService.AcquirerId });

            connection.Execute(@"
insert into lock(resource, acquired, acquirer)
values ('remote_instance_lock', (now() - interval '5 second') at time zone 'UTC', 'instance_lock_id_1')");

            using var lockHolder = LockService.AcquireLock("unique_resource", TimeSpan.Zero);

            var locksCount = connection.FetchScalar<int>("select count(1) from lock");

            Assert.Equal(2, locksCount);

            connection.Execute(@"delete from server where id = 'remote_instance'");
            
#pragma warning disable 618
            (LockService as IServerComponent)?.Execute(CancellationToken.None);
#pragma warning restore 618

            locksCount = connection.FetchScalar<int>("select count(1) from lock");
            Assert.Equal(1, locksCount);
        }
        
        
        [Fact]
        public void Execute_RemovesOnlyExpiredLocks()
        {
            UseConnection((provider, connection) =>
            {
                // Arrange
                var timeout = TimeSpan.FromSeconds(5);
                connection.Execute(@"insert into server(id, lastheartbeat, lockacquirerid) values ('alive_server', now() at time zone 'UTC', @lockId)", new
                {
                    lockId = LockService.AcquirerId
                });
                connection.Execute(
                    @"insert into lock(resource, acquired, acquirer) values ('fresh_lock', now() at time zone 'UTC', @lockId)", new
                    {
                        
                        lockId = LockService.AcquirerId
                    });
                connection.Execute(
                    @"insert into lock(resource, acquired, acquirer) values ('expired_lock', now() at time zone 'UTC' - @timeout, 'dead_server')",
                    new { timeout });

                // Act
                var expiredLocksManager = new LockService(provider, timeout - TimeSpan.FromSeconds(1));
                expiredLocksManager.Execute(CancellationToken.None);

                // Assert
                var locksCount = connection.ExecuteScalar<int>(@"select count(*) from lock");
                Assert.Equal(1, locksCount);
            });
        }

        private void UseConnection(Action<IConnectionProvider, NpgsqlConnection> action)
        {
            using var connection = ConnectionProvider.AcquireConnection();
            action(ConnectionProvider, connection.Connection);
        }

        public LockServiceIntegrationTests(StorageContext<LockServiceIntegrationTests> storageContext, ITestOutputHelper testOutputHelper)
            : base(storageContext, testOutputHelper) { }
    }
}
