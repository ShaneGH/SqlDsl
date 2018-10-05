using System.Collections.Generic;
using System.Threading.Tasks;

namespace SqlDsl
{
    /// <summary>
    /// An object to execute sql with extra debug parameters
    /// </summary>
    public interface IDebugExecutor : IExecutor
    {
        /// <summary>
        /// Execute a sql query and return a reader to read results
        /// </summary>
        Task<IReader> ExecuteAsync(string sql, IEnumerable<object> paramaters, string[] columnNames);
        
        /// <summary>
        /// Execute a sql query and return a reader to read results
        /// </summary>
        IReader Execute(string sql, IEnumerable<object> paramaters, string[] columnNames);
    }
}
