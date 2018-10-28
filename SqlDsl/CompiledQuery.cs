using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SqlDsl.DataParser;
using SqlDsl.SqlBuilders;

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
        Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        IEnumerable<TResult> Execute(IExecutor executor, ILogger logger = null);
    }

    /// <summary>
    /// A query compiled for reuse
    /// </summary>
    public interface ICompiledQuery<TArgs, TResult>
    {
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        IEnumerable<TResult> Execute(IExecutor executor, TArgs args, ILogger logger = null);
    }

    public class CompiledQuery<TArgs, TResult> : ICompiledQuery<TArgs, TResult>
    {
        internal readonly string Sql;
        readonly IEnumerable<object> Parameters;
        readonly string[] SelectColumns;
        public readonly RootObjectPropertyGraph PropertyGraph;

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

        public async Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            // build sql params
            var parameters = Parameters.Select(p =>
                p is IQueryArgAccessor<TArgs> ? 
                    (p as IQueryArgAccessor<TArgs>).GetArgValue(args) :
                    p);

            // execute and get all rows
            var reader = await executor.ExecuteDebugAsync(Sql, parameters, SelectColumns);
            var results = await reader.GetRowsAsync();

            return results.Parse<TResult>(PropertyGraph, logger);
        }

        public IEnumerable<TResult> Execute(IExecutor executor, TArgs args, ILogger logger = null)
        {
            // build sql params
            var parameters = Parameters.Select(p =>
                p is IQueryArgAccessor<TArgs> ? 
                    (p as IQueryArgAccessor<TArgs>).GetArgValue(args) :
                    p);
                    
            // execute and get all rows
            var reader = executor.ExecuteDebug(Sql, Parameters, SelectColumns);
            var results = reader.GetRows();

            return results.Parse<TResult>(PropertyGraph, logger);
        }
    }

    public class CompiledQuery<TResult> : ICompiledQuery<TResult>
    {
        /// <summary>
        /// Debug only (TODO: make internal)
        /// </summary>
        public RootObjectPropertyGraph PropertyGraph => (Worker as CompiledQuery<object, TResult>)?.PropertyGraph;
        
        /// <summary>
        /// Debug only (TODO: make internal)
        /// </summary>
        public string Sql => (Worker as CompiledQuery<object, TResult>)?.Sql;

        readonly ICompiledQuery<object, TResult> Worker;

        public CompiledQuery(ICompiledQuery<object, TResult> worker)
        {
            Worker = worker ?? throw new ArgumentNullException(nameof(worker));
        }

        public Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor, ILogger logger = null) => Worker.ExecuteAsync(executor, null, logger: logger);

        public IEnumerable<TResult> Execute(IExecutor executor, ILogger logger = null) => Worker.Execute(executor, null, logger: logger);
    }
}
