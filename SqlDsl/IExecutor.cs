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
}
