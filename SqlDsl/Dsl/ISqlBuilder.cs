using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Dsl
{    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface ISqlBuilder<TResult>
    {        
        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        /// <param name="logger">A logger to capture info and warning messages</param>
        Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor, ILogger logger = null);

        // TODO: add a ToListAsync, ToArrayAsync, ToLazyAsync
        
        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        /// <param name="logger">A logger to capture info and warning messages</param>
        IEnumerable<TResult> Execute(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        /// <param name="logger">A logger to capture info and warning messages</param>
        ICompiledQuery<TResult> Compile(ILogger logger = null);
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface ISqlBuilder<TArgs, TResult>
    {        
        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        /// <param name="args">
        /// The args needed to execute the query
        /// </param>
        /// <param name="logger">A logger to capture info and warning messages</param>
        Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        /// <param name="args">
        /// The args needed to execute the query
        /// </param>
        /// <param name="logger">A logger to capture info and warning messages</param>
        IEnumerable<TResult> Execute(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        /// <param name="logger">A logger to capture info and warning messages</param>
        ICompiledQuery<TArgs, TResult> Compile(ILogger logger = null);
    }
}
