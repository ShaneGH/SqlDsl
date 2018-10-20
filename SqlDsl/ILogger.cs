using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SqlDsl
{
    /// <summary>
    /// A logger to record trace and warning information
    /// </summary>
    public interface ILogger
    {
        /// <summary>
        /// Log an info message
        /// </summary>
        /// <param name="message">The message to log</param>
        void Info(string message);

        /// <summary>
        /// Log a warning message
        /// </summary>
        /// <param name="message">The message to log</param>
        void Warning(string message);

        /// <summary>
        /// The minimum level to log at
        /// </summary>
        LogLevel LogLevel { get; }
    }

    public enum LogLevel
    {
        Info = 1,
        Warning = 2,
        DoNotLog = 3
    }
    
    internal static class ILoggerUtils
    {
        static readonly int InfoLevel = (int)LogLevel.Info;
        static readonly int WarningLevel = (int)LogLevel.Warning;

        /// <summary>
        /// Determine whether a logger can log an info message
        /// </summary>
        public static bool CanLogInfo(this ILogger logger)
        {
            return logger != null && (int)logger.LogLevel <= InfoLevel;
        }

        /// <summary>
        /// Determine whether a logger can log a warning
        /// </summary>
        public static bool CanLogWarning(this ILogger logger)
        {
            return logger != null && (int)logger.LogLevel <= WarningLevel;
        }

        const string SqlDslPrefix = "[SqlDsl] ";

        /// <summary>
        /// Log an info message
        /// </summary>
        public static void LogInfo(this ILogger logger, string message)
        {
            logger.Info($"{SqlDslPrefix}{message}");
        }

        /// <summary>
        /// Log a warning message
        /// </summary>
        public static void LogWarning(this ILogger logger, string message)
        {
            logger.Warning($"{SqlDslPrefix}{message}");
        }
    }
}
