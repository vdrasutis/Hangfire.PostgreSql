using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Resources;
using Dapper;
using Hangfire.Logging;
using Npgsql;

namespace Hangfire.PostgreSql
{
    internal static class DatabaseInitializer
    {
        private static readonly ILog Log = LogProvider.GetLogger(typeof(PostgreSqlStorage));

        public static void Initialize(NpgsqlConnection connection, string schemaName = "hangfire")
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            Log.Info("Start installing Hangfire SQL objects...");

            // starts with version 3 to keep in check with Hangfire SqlServer, but I couldn't keep up with that idea after all;
            int version = 3;
            int previousVersion = 1;
            do
            {
                try
                {
                    string script = null;
                    try
                    {
                        script = GetStringResource(
                            typeof(DatabaseInitializer).GetTypeInfo().Assembly,
                            $"Hangfire.PostgreSql.Schema.Install.v{version.ToString(CultureInfo.InvariantCulture)}.sql");
                    }
                    catch (MissingManifestResourceException)
                    {
                        break;
                    }

                    if (schemaName != "hangfire")
                    {
                        script = script.Replace("'hangfire'", $"'{schemaName}'")
                            .Replace(@"""hangfire""", $@"""{schemaName}""");
                    }

                    using (var transaction = connection.BeginTransaction(IsolationLevel.Serializable))
                    using (var command = new NpgsqlCommand(script, connection, transaction))
                    {
                        command.CommandTimeout = 120;
                        try
                        {
                            command.ExecuteNonQuery();

                            // Due to https://github.com/npgsql/npgsql/issues/641 , it's not possible to send
                            // CREATE objects and use the same object in the same command
                            // So bump the version in another command
                            connection.Execute(
                                $@"UPDATE ""{
                                        schemaName
                                    }"".""schema"" SET ""version"" = @version WHERE ""version"" = @previousVersion",
                                new {version, previousVersion}, transaction);

                            transaction.Commit();
                        }
                        catch (PostgresException ex)
                        {
                            if ((ex.MessageText ?? "") != "version-already-applied")
                            {
                                throw;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (ex.Source.Equals("Npgsql"))
                    {
                        Log.ErrorException("Error while executing install/upgrade", ex);
                    }
                }

                previousVersion = version;
                version++;
            } while (true);

            Log.Info("Hangfire SQL objects installed.");
        }

        private static string GetStringResource(Assembly assembly, string resourceName)
        {
            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                if (stream == null)
                {
                    throw new MissingManifestResourceException(
                        $"Requested resource `{resourceName}` was not found in the assembly `{assembly}`.");
                }

                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}
