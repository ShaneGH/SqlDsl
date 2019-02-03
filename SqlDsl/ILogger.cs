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
        /// Log a debug message
        /// </summary>
        /// <param name="message">The message to log</param>
        void Debug(string message);

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
        Debug = 1,
        Info = 2,
        Warning = 3,
        DoNotLog = 4
    }

    public enum LogMessages
    {
        /// <summary>
        /// A log message which is fired when a new ObjectGraph is created
        /// </summary>
        CreatedObjectGraphAllocation = 10000,

        /// <summary>
        /// A log message which is fired when a new ObjectGraph is created
        /// </summary>
        CreatedPropMapValueAllocation = 10001,

        /// <summary>
        /// A log message which confirms a query was compiled and prints the time taken
        /// </summary>
        CompiledQuery = 20000,

        /// <summary>
        /// A log message which is records the sql query executed
        /// </summary>
        ExecutingQuery = 20001,

        /// <summary>
        /// A log message which confirms a query was executed and prints the time taken
        /// </summary>
        ExecutedQuery = 20002,

        /// <summary>
        /// A log message which confirms a query was parsed and prints the time taken
        /// </summary>
        ParsedQuery = 20003,
        
        /// <summary>
        /// A log message which is fired when value types need to be casted and boxed
        /// </summary>
        InefficientCastWarning = 30000
    }
    
    internal static class ILoggerUtils
    {
        static readonly int DebugLevel = (int)LogLevel.Debug;
        static readonly int InfoLevel = (int)LogLevel.Info;
        static readonly int WarningLevel = (int)LogLevel.Warning;

        /// <summary>
        /// Determine whether a logger can log an info message
        /// </summary>
        public static bool CanLogDebug(this ILogger logger, LogMessages? messageType)
        {
            return logger != null && 
                (int)logger.LogLevel <= DebugLevel &&
                (messageType == null || !logger.SupressLogMessages.Contains(messageType.Value));
        }

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
        /// Log a debug message
        /// </summary>
        public static void LogDebug(this ILogger logger, string message, LogMessages? messageType)
        {
            logger.Debug($"{SqlDslPrefix(messageType)}{message}");
        }

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
