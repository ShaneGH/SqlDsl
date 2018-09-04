using System.Collections.Generic;
using System.Threading.Tasks;

namespace SqlDsl
{
    /// <summary>
    /// An object which can execute a sql query
    /// </summary>
    public interface IExecutor
    {
        /// <summary>
        /// Execute a sql query and return a reader to read results
        /// </summary>
        Task<IReader> ExecuteAsync(string sql, IEnumerable<object> paramaters);
    }

    public static class IExecutorUtils
    {
        /// <summary>
        /// If the input is a debug executor, run ExecuteAsync with column names,
        /// otherwise use IExecutor.ExecuteAsync
        /// </summary>
        public static Task<IReader> ExecuteDebugAsync(this IExecutor executor, string sql, IEnumerable<object> paramaters, string[] columnNames) =>
            executor is IDebugExecutor ?
                (executor as IDebugExecutor).ExecuteAsync(sql, paramaters, columnNames) :
                executor.ExecuteAsync(sql, paramaters);
    }
}
