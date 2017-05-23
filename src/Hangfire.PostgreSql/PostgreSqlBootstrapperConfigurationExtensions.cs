namespace Hangfire.PostgreSql
{
    public static class PostgreSqlBootstrapperConfigurationExtensions
    {
        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage,
        /// that can be accessed using the given connection string or 
        /// its name.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="nameOrConnectionString">Connection string or its name</param>
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this GlobalConfiguration configuration,
            string nameOrConnectionString)
        {
            var storage = new PostgreSqlStorage(nameOrConnectionString);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage
        /// with the given options, that can be accessed using the specified
        /// connection string or its name.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="nameOrConnectionString">Connection string or its name</param>
        /// <param name="options">Advanced options</param>
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this GlobalConfiguration configuration,
            string nameOrConnectionString,
            PostgreSqlStorageOptions options)
        {
            var storage = new PostgreSqlStorage(nameOrConnectionString, options);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage,
        /// that can be accessed using the given connection string or 
        /// its name.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="nameOrConnectionString">Connection string or its name</param>
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this IGlobalConfiguration configuration,
            string nameOrConnectionString)
        {
            var storage = new PostgreSqlStorage(nameOrConnectionString);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage
        /// with the given options, that can be accessed using the specified
        /// connection string or its name.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="nameOrConnectionString">Connection string or its name</param>
        /// <param name="options">Advanced options</param>
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this IGlobalConfiguration configuration,
            string nameOrConnectionString,
            PostgreSqlStorageOptions options)
        {
            var storage = new PostgreSqlStorage(nameOrConnectionString, options);
            configuration.UseStorage(storage);

            return storage;
        }
    }
}
