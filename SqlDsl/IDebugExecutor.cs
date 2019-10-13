using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
        Task<DbDataReader> ExecuteAsync(string sql, IEnumerable<(string name, object value)> paramaters, string[] columnNames);
        
        /// <summary>
        /// Execute a sql query and return a reader to read results
        /// </summary>
        DbDataReader Execute(string sql, IEnumerable<(string name, object value)> paramaters, string[] columnNames);
    }
}
