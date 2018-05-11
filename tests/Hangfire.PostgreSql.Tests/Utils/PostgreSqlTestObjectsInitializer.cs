using System;
using System.Data;
using System.IO;
using System.Reflection;
using Npgsql;

namespace Hangfire.PostgreSql.Tests.Utils
{
    internal static class PostgreSqlTestObjectsInitializer
    {
        public static void CleanTables(NpgsqlConnection connection)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));

            var script = GetStringResource(
                typeof(PostgreSqlTestObjectsInitializer).GetTypeInfo().Assembly,
                "Hangfire.PostgreSql.Tests.Clean.sql");

            //connection.Execute(script);

            using (var transaction = connection.BeginTransaction(IsolationLevel.Serializable))
            using (var command = new NpgsqlCommand(script, connection, transaction))
            {
                command.CommandTimeout = 120;
                try
                {
                    command.ExecuteNonQuery();
                    transaction.Commit();
                }
                catch (NpgsqlException)
                {
                    throw;
                }
            }
        }

        private static string GetStringResource(Assembly assembly, string resourceName)
        {
            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                if (stream == null)
                {
                    throw new InvalidOperationException(string.Format(
                        "Requested resource `{0}` was not found in the assembly `{1}`.",
                        resourceName,
                        assembly));
                }

                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}
