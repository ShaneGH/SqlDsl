using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SqlDsl.DataParser;

namespace SqlDsl
{
    /// <summary>
    /// A query compiled for reuse
    /// </summary>
    public interface ICompiledQuery<TResult>
    {
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        IEnumerable<TResult> Execute(IExecutor executor);
    }

    /// <summary>
    /// A query compiled for reuse
    /// </summary>
    public interface ICompiledQuery<TArgs, TResult>
    {
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor, TArgs args);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        IEnumerable<TResult> Execute(IExecutor executor, TArgs args);
    }

    public class CompiledQuery<TResult> : ICompiledQuery<TResult>
    {
        readonly string Sql;
        readonly IEnumerable<object> Parameters;
        readonly string[] SelectColumns;
        readonly RootObjectPropertyGraph PropertyGraph;

        public CompiledQuery(
            string sql, 
            IEnumerable<object> parameters, 
            string[] selectColumns,
            RootObjectPropertyGraph propertyGraph)
        {
            Sql = sql;
            Parameters = parameters;
            SelectColumns = selectColumns;
            PropertyGraph = propertyGraph;
        }

        public async Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor)
        {
            // execute and get all rows
            var reader = await executor.ExecuteDebugAsync(Sql, Parameters, SelectColumns);
            var results = await reader.GetRowsAsync();

            return results.Parse<TResult>(PropertyGraph);
        }

        public IEnumerable<TResult> Execute(IExecutor executor)
        {
            // execute and get all rows
            var reader = executor.ExecuteDebug(Sql, Parameters, SelectColumns);
            var results = reader.GetRows();

            return results.Parse<TResult>(PropertyGraph);
        }
    }
}
