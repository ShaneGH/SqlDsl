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

        /// <summary>
        /// A set of log messages to supress
        /// </summary>
        HashSet<LogMessages> SupressLogMessages { get; }
    }

    public enum LogLevel
    {
        Info = 1,
        Warning = 2,
        DoNotLog = 3
    }

    public enum LogMessages
    {
        /// <summary>
        /// A log message which is fired when value types need to be casted and boxed
        /// </summary>
        InefficientCastWarning = 10000
    }
    
    internal static class ILoggerUtils
    {
        static readonly int InfoLevel = (int)LogLevel.Info;
        static readonly int WarningLevel = (int)LogLevel.Warning;

        /// <summary>
        /// Determine whether a logger can log an info message
        /// </summary>
        public static bool CanLogInfo(this ILogger logger, LogMessages? messageType)
        {
            return logger != null && 
                (int)logger.LogLevel <= InfoLevel &&
                (messageType == null || !logger.SupressLogMessages.Contains(messageType.Value));
        }

        /// <summary>
        /// Determine whether a logger can log a warning
        /// </summary>
        public static bool CanLogWarning(this ILogger logger, LogMessages? messageType)
        {
            return logger != null && 
                (int)logger.LogLevel <= WarningLevel &&
                (messageType == null || !logger.SupressLogMessages.Contains(messageType.Value));
        }

        static string SqlDslPrefix(LogMessages? messageType) => messageType == null ?
            "[SqlDsl] " :
            $"[SqlDsl, {(int)messageType.Value}] ";

        /// <summary>
        /// Log an info message
        /// </summary>
        public static void LogInfo(this ILogger logger, string message, LogMessages? messageType)
        {
            logger.Info($"{SqlDslPrefix(messageType)}{message}");
        }

        /// <summary>
        /// Log a warning message
        /// </summary>
        public static void LogWarning(this ILogger logger, string message, LogMessages? messageType)
        {
            logger.Warning($"{SqlDslPrefix(messageType)}{message}");
        }
    }
}
