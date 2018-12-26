using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Dsl
{    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface ISqlExecutor<TResult>
    {
        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        /// <param name="logger">A logger to capture info and warning messages</param>
        Task<IEnumerable<TResult>> ToIEnumerableAsync(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        /// <param name="logger">A logger to capture info and warning messages</param>
        IEnumerable<TResult> ToIEnumerable(IExecutor executor, ILogger logger = null);

        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        /// <param name="logger">A logger to capture info and warning messages</param>
        Task<List<TResult>> ToListAsync(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        /// <param name="logger">A logger to capture info and warning messages</param>
        List<TResult> ToList(IExecutor executor, ILogger logger = null);

        /// <summary>
        /// Execute the sql query and get an array of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        /// <param name="logger">A logger to capture info and warning messages</param>
        Task<TResult[]> ToArrayAsync(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the sql query and get an array of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        /// <param name="logger">A logger to capture info and warning messages</param>
        TResult[] ToArray(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        /// <param name="logger">A logger to capture info and warning messages</param>
        ICompiledQuery<TResult> Compile(ILogger logger = null);
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface ISqlExecutor<TArgs, TResult>
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
        Task<IEnumerable<TResult>> ToIEnumerableAsync(IExecutor executor, TArgs args, ILogger logger = null);
        
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
        IEnumerable<TResult> ToIEnumerable(IExecutor executor, TArgs args, ILogger logger = null);
        
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
        Task<List<TResult>> ToListAsync(IExecutor executor, TArgs args, ILogger logger = null);
        
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
        List<TResult> ToList(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the sql query and get an array of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        /// <param name="args">
        /// The args needed to execute the query
        /// </param>
        /// <param name="logger">A logger to capture info and warning messages</param>
        Task<TResult[]> ToArrayAsync(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the sql query and get an array of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        /// <param name="args">
        /// The args needed to execute the query
        /// </param>
        /// <param name="logger">A logger to capture info and warning messages</param>
        TResult[] ToArray(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        /// <param name="logger">A logger to capture info and warning messages</param>
        ICompiledQuery<TArgs, TResult> Compile(ILogger logger = null);
    }
}
