using System;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Npgsql;

namespace Hangfire.PostgreSql.Tests.Setup
{
    // ReSharper disable once UnusedTypeParameter
    public sealed class StorageContext<T> : IDisposable
        where T : StorageContextBasedTests<T>
    {
        public string RunId { get; }
        public string DatabaseName { get; }
        public string SchemaName { get; }
        public string ConnectionString { get; }
        public PostgreSqlStorage Storage { get; }

        public StorageContext()
        {
            RunId = Guid.NewGuid().ToString("N");

            var templateConnectionString = ConnectivityUtilities.GetConnectionString();

            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(templateConnectionString)
            {
                SearchPath = "hangfire_test_" + RunId
            };

            var connectionString = connectionStringBuilder.ConnectionString;

            Guard.ThrowIfConnectionStringIsInvalid(connectionString);
            DatabaseName = connectionStringBuilder.Database!;
            SchemaName = connectionStringBuilder.SearchPath;
            ConnectionString = connectionString;

            TryCreateDatabase(connectionStringBuilder);

            Storage = new PostgreSqlStorage(connectionString,
                new PostgreSqlStorageOptions
                {
                    PrepareSchemaIfNecessary = true,
                    DistributedLockTimeout = TimeSpan.FromSeconds(1)
                });
        }

        private void TryCreateDatabase(NpgsqlConnectionStringBuilder connectionStringBuilder)
        {
            connectionStringBuilder.Database = "postgres";
            connectionStringBuilder.SearchPath = null;

            using (var connection = new NpgsqlConnection(connectionStringBuilder.ConnectionString))
            {
                connection.Open();
                var databaseExists = connection.ExecuteScalar<int>(
                    @"select count(*) from pg_database where datname = @databaseName;",
                    new { databaseName = DatabaseName }) > 0;

                if (!databaseExists)
                {
                    connection.Execute(@$"create database {DatabaseName}");
                }
            }

            connectionStringBuilder.Database = DatabaseName;
            using (var connection = new NpgsqlConnection(connectionStringBuilder.ConnectionString))
            {
                connection.Open();
                connection.Execute(@$"create schema {SchemaName}");
            }
        }

        public void TruncateTables()
        {
            const string cleanQuery = @"
truncate counter cascade;
truncate hash cascade;
truncate job cascade;
truncate jobparameter cascade;
truncate jobqueue cascade;
truncate list cascade;
truncate lock cascade;
truncate server cascade;
truncate set cascade;
truncate state cascade;
";
            
            Storage.ConnectionProvider.Execute(cleanQuery);
        }

        public void Dispose()
        {
            Storage.ConnectionProvider.Execute(@$"drop schema {SchemaName} cascade;");
            Storage.Dispose();
        }
    }
}
