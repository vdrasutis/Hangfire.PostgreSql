using Hangfire.Annotations;

namespace Hangfire.PostgreSql
{
    public static class PostgreSqlBootstrapperConfigurationExtensions
    {
        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage,
        /// that can be accessed using the given connection string.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string</param>
        [PublicAPI]
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this GlobalConfiguration configuration,
            string connectionString)
        {
            var storage = new PostgreSqlStorage(connectionString);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage
        /// with the given options, that can be accessed using the specified
        /// connection string.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="options">Advanced options</param>
        [PublicAPI]
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this GlobalConfiguration configuration,
            string connectionString,
            PostgreSqlStorageOptions options)
        {
            var storage = new PostgreSqlStorage(connectionString, options);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage,
        /// that can be accessed using the given connection string.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string</param>
        [PublicAPI]
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this IGlobalConfiguration configuration,
            string connectionString)
        {
            var storage = new PostgreSqlStorage(connectionString);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage
        /// with the given options, that can be accessed using the specified
        /// connection string or its name.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="options">Advanced options</param>
        [PublicAPI]
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this IGlobalConfiguration configuration,
            string connectionString,
            PostgreSqlStorageOptions options)
        {
            var storage = new PostgreSqlStorage(connectionString, options);
            configuration.UseStorage(storage);

            return storage;
        }
    }
}
