using System;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Tests.Utils;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class CountersAggregationManagerFacts
    {
        private readonly CancellationToken _token;
        private readonly PostgreSqlStorageOptions _options;

        public CountersAggregationManagerFacts()
        {
            var cts = new CancellationTokenSource();
            _token = cts.Token;
            _options = new PostgreSqlStorageOptions();
        }

        [Fact, CleanDatabase]
        public void Execute_Aggregates_CounterTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                var createSql = $@"
insert into ""counter"" (""key"", ""value"") 
values ('stats:succeeded', 1)";
                for (int i = 0; i < 5; i++)
                {
                    connection.Execute(createSql);
                }

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(1,
                    connection.Query<long>(@"select count(*) from ""counter""").Single());
                Assert.Equal(5,
                    connection.Query<long>(@"select sum(value) from ""counter""")
                        .Single());
            }
        }

        private NpgsqlConnection CreateConnection()
        {
            return ConnectionUtils.CreateNpgConnection();
        }

        private CountersAggregationManager CreateManager()
        {
            var connectionProvider = ConnectionUtils.CreateConnection();
            return new CountersAggregationManager(connectionProvider, TimeSpan.FromSeconds(1));
        }
    }
}
