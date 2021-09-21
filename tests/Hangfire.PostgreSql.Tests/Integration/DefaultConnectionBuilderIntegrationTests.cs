using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Setup;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests.Integration
{
    public class DefaultConnectionBuilderIntegrationTests
    {
        [Fact]
        public void ConnectionStringBuilder_Is_Readable()
        {
            var connectionString = ConnectivityUtilities.GetConnectionString();
            var builder = new DefaultConnectionBuilder(connectionString);

            Assert.NotNull(builder.ConnectionStringBuilder);
            Assert.NotNull(builder.ConnectionStringBuilder.ConnectionString);
        }

        [Fact]
        public void Build_CreatesConnection()
        {
            var builder = new DefaultConnectionBuilder(ConnectivityUtilities.GetConnectionString());

            using var connection = builder.Build();
            Assert.NotNull(connection);
        }

        [Fact]
        public void Build_CreatedFixedUpConnection()
        {
            void FixUpAction(NpgsqlConnection conn)
                => conn.UserCertificateValidationCallback += (sender, cert, chain, errors) => true;

            var builder = new DefaultConnectionBuilder(ConnectivityUtilities.GetConnectionString(), FixUpAction);

            using var connection = builder.Build();
            Assert.NotNull(connection);
            Assert.NotNull(connection.UserCertificateValidationCallback);
        }
    }
}
