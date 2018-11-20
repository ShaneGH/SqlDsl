using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SqlDsl.DataParser;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

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
        Task<IEnumerable<TResult>> ToIEnumerableAsync(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        IEnumerable<TResult> ToIEnumerable(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<List<TResult>> ToListAsync(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        List<TResult> ToList(IExecutor executor, ILogger logger = null);
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<TResult[]> ToArrayAsync(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        TResult[] ToArray(IExecutor executor, ILogger logger = null);
    }

    /// <summary>
    /// A query compiled for reuse
    /// </summary>
    public interface ICompiledQuery<TArgs, TResult>
    {
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<IEnumerable<TResult>> ToIEnumerableAsync(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        IEnumerable<TResult> ToIEnumerable(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<List<TResult>> ToListAsync(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        List<TResult> ToList(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<TResult[]> ToArrayAsync(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        TResult[] ToArray(IExecutor executor, TArgs args, ILogger logger = null);
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

        // <inheritdoc />
        public async Task<IEnumerable<TResult>> ToIEnumerableAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            // build sql params
            var parameters = Parameters.Select(p =>
                p is IQueryArgAccessor<TArgs> ? 
                    (p as IQueryArgAccessor<TArgs>).GetArgValue(args) :
                    p);
                    
            if (logger.CanLogInfo(LogMessages.ExecutingQuery))
                logger.LogInfo($"Executing sql:{Environment.NewLine}{Sql}", LogMessages.ExecutingQuery);

            var timer = new Timer(true);

            // execute and get all rows
            var reader = await executor.ExecuteDebugAsync(Sql, parameters, SelectColumns);
            var results = await reader.GetRowsAsync();

            if (logger.CanLogInfo(LogMessages.ExecutedQuery))
                logger.LogInfo($"Executed sql in {timer.SplitString()}", LogMessages.ExecutedQuery);

            return results.Parse<TResult>(PropertyGraph, logger);
        }

        // <inheritdoc />
        public IEnumerable<TResult> ToIEnumerable(IExecutor executor, TArgs args, ILogger logger = null)
        {
            // build sql params
            var parameters = Parameters.Select(p =>
                p is IQueryArgAccessor<TArgs> ? 
                    (p as IQueryArgAccessor<TArgs>).GetArgValue(args) :
                    p);
                    
            if (logger.CanLogInfo(LogMessages.ExecutingQuery))
                logger.LogInfo($"Executing sql:{Environment.NewLine}{Sql}", LogMessages.ExecutingQuery);

            var timer = new Timer(true);

            // execute and get all rows
            var reader = executor.ExecuteDebug(Sql, Parameters, SelectColumns);
            var results = reader.GetRows();

            if (logger.CanLogInfo(LogMessages.ExecutedQuery))
                logger.LogInfo($"Executed sql in {timer.SplitString()}", LogMessages.ExecutedQuery);

            return results.Parse<TResult>(PropertyGraph, logger);
        }

        // <inheritdoc />
        public async Task<List<TResult>> ToListAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var enumerable = await ToIEnumerableAsync(executor, args, logger);
            var result = enumerable.ToList();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Query parsed in {timer.SplitString()}", LogMessages.ParsedQuery);

            return result;
        }

        // <inheritdoc />
        public List<TResult> ToList(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var enumerable = ToIEnumerable(executor, args, logger);
            var result = enumerable.ToList();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Query parsed in {timer.SplitString()}", LogMessages.ParsedQuery);
            
            return result;
        }

        // <inheritdoc />
        public async Task<TResult[]> ToArrayAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var enumerable = await ToIEnumerableAsync(executor, args, logger);
            var result = enumerable.ToArray();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Query parsed in {timer.SplitString()}", LogMessages.ParsedQuery);

            return result;
        }

        // <inheritdoc />
        public TResult[] ToArray(IExecutor executor, TArgs args, ILogger logger = null)
        {
            var timer = new Timer(true);
            var enumerable = ToIEnumerable(executor, args, logger);
            var result = enumerable.ToArray();
            
            if (logger.CanLogInfo(LogMessages.ParsedQuery))
                logger.LogInfo($"Query parsed in {timer.SplitString()}", LogMessages.ParsedQuery);

            return result;
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

        // <inheritdoc />
        public Task<IEnumerable<TResult>> ToIEnumerableAsync(IExecutor executor, ILogger logger = null) => Worker.ToIEnumerableAsync(executor, null, logger: logger);

        // <inheritdoc />
        public IEnumerable<TResult> ToIEnumerable(IExecutor executor, ILogger logger = null) => Worker.ToIEnumerable(executor, null, logger: logger);

        // <inheritdoc />
        public async Task<List<TResult>> ToListAsync(IExecutor executor, ILogger logger = null) => (await ToIEnumerableAsync(executor, logger)).ToList();

        // <inheritdoc />
        public List<TResult> ToList(IExecutor executor, ILogger logger = null) => ToIEnumerable(executor, logger).ToList();

        // <inheritdoc />
        public async Task<TResult[]> ToArrayAsync(IExecutor executor, ILogger logger = null) => (await ToIEnumerableAsync(executor, logger)).ToArray();

        // <inheritdoc />
        public TResult[] ToArray(IExecutor executor, ILogger logger = null) => ToIEnumerable(executor, logger).ToArray();
    }
}
