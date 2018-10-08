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
        Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor);
        
        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        IEnumerable<TResult> Execute(IExecutor executor);
        
        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        ICompiledQuery<TResult> Compile();
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface ISqlBuilder<TArgs, TResult>
    {
        /// <summary>
        /// Get a sql statement and corresponding sql paramaters from the query
        /// </summary>
        (string sql, IEnumerable<object> paramaters) ToSql();
        //TODO: try to remove this method. It is confusing. (Should the method not accept a TArgs parameter)
        
        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        /// <param name="args">
        /// The args needed to execute the query
        /// </param>
        Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor, TArgs args);
        
        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        /// <param name="args">
        /// The args needed to execute the query
        /// </param>
        IEnumerable<TResult> Execute(IExecutor executor, TArgs args);
        
        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        ICompiledQuery<TArgs, TResult> Compile();
    }
}
