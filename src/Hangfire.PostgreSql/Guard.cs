using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Hangfire.Annotations;
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

        [ContractAnnotation("argument:null => halt")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfNull([NotNull] object argument, [NotNull] string argumentName)
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
        public static void ThrowIfConnectionStringIsInvalid([NotNull] string connectionString)
        {
            ThrowIfNull(connectionString, nameof(connectionString));
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString);
            if (connectionStringBuilder.Host == null) throw new ArgumentException(HostHasNotFoundExceptionMessage);
            if (connectionStringBuilder.Enlist) throw new ArgumentException(EnlistIsNotAvailableExceptionMessage);
            if (connectionStringBuilder.Pooling) throw new ArgumentException(PoolingIsNotAvailableExceptionMessage);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfValueIsNotPositive(int value, string fieldName)
        {
            if (value <= 0)
            {
                var message = $"The {fieldName} property value should be positive. Given: {value}.";
                throw new ArgumentException(message, nameof(value));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfValueIsNotPositive(TimeSpan value, string fieldName)
        {
            var message = $"The {fieldName} property value should be positive. Given: {value}.";

            if (value == TimeSpan.Zero)
            {
                throw new ArgumentException(message, nameof(value));
            }
            if (value != value.Duration())
            {
                throw new ArgumentException(message, nameof(value));
            }
        }
    }
}
