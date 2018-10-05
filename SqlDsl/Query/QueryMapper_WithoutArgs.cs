using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Query
{
    /// <summary>
    /// Wrapper for ISqlBuilder&lt;object, TMapped> which ignores args
    /// </summary>
    public class QueryMapper<TMapped> : ISqlBuilder<TMapped>
    {
        readonly ISqlBuilder<object, TMapped> Worker;
        
        public QueryMapper(ISqlBuilder<object, TMapped> worker)
        {
            Worker = worker ?? throw new ArgumentNullException(nameof(worker));
        }

        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        public ICompiledQuery<TMapped> Compile()
        {
            return new CompiledQuery<TMapped>(Worker.Compile());
        }

        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        public Task<IEnumerable<TMapped>> ExecuteAsync(IExecutor executor)
        {
            return Worker.ExecuteAsync(executor, null);
        }

        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        public IEnumerable<TMapped> Execute(IExecutor executor)
        {
            return Worker.Execute(executor, null);
        }

        /// <summary>
        /// Get a sql statement and corresponding sql paramaters from the query
        /// </summary>
        public (string sql, IEnumerable<object> paramaters) ToSql()
        {
            return Worker.ToSql();
        }
    }
}
