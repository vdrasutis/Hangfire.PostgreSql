using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Hangfire.PostgreSql.Properties;
using Npgsql;

namespace Hangfire.PostgreSql
{
    [DebuggerStepThrough]
    internal static class Guard
    {
        private const string EnlistIsNotAvailableExceptionMessage =
                "Npgsql is not fully compatible with TransactionScope yet, only connections without Enlist = true are accepted.";
        private const string HostHasNotFoundExceptionMessage = "Invalid Postgres connection string: host has not found.";
        private const string PoolingIsNotAvailableExceptionMessage = "Pooling=true can't be used in connection string.";

        [ContractAnnotation("condition:false => halt")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfNot(bool condition, [NotNull] string argumentName)
        {
            if (!condition)
                throw new ArgumentException("Condition failed", argumentName);
        }

        [ContractAnnotation("objects:null => halt")]
        public static void ThrowIfItemIsNull<T>([NotNull] IReadOnlyCollection<T> objects, [NotNull] string argumentName)
            where T : class
        {
            ThrowIfNull(objects, nameof(objects));
            foreach (var @object in objects)
            {
                if (@object == null)
                    throw new ArgumentException("Item cannot be null", argumentName);
            }
        }

        [ContractAnnotation("argument:null => halt")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfNull([NoEnumeration] [NotNull] object argument, [NotNull] string argumentName)
        {
            if (argument == null)
                throw new ArgumentNullException(argumentName);
        }

        [ContractAnnotation("argument:null => halt")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfNullOrEmpty([NotNull] string @string, [NotNull] string argumentName)
        {
            if (string.IsNullOrEmpty(@string))
                throw new ArgumentNullException(argumentName);
        }

        [ContractAnnotation("argument:null => halt")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfNullOrWhitespace([NotNull] string @string, [NotNull] string argumentName)
        {
            if (string.IsNullOrWhiteSpace(@string))
                throw new ArgumentException("Parameter cannot be null or whitespace string", argumentName);
        }

        public static void ThrowIfConnectionStringIsInvalid([NotNull] string connectionString)
        {
            ThrowIfNull(connectionString, nameof(connectionString));
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString);
            if (connectionStringBuilder.Host == null) throw new ArgumentException(HostHasNotFoundExceptionMessage);
            if (connectionStringBuilder.Enlist) throw new ArgumentException(EnlistIsNotAvailableExceptionMessage);
            if (connectionStringBuilder.Pooling) throw new ArgumentException(PoolingIsNotAvailableExceptionMessage);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfConnectionContainsEnlist([NotNull] NpgsqlConnection connection)
        {
            ThrowIfNull(connection, nameof(connection));
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connection.ConnectionString);
            if (connectionStringBuilder.Enlist) throw new ArgumentException(EnlistIsNotAvailableExceptionMessage);
        }
    }
}
