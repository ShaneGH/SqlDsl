using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Dsl
{    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IFilter<TResult> : IResultMapper<TResult>
    {
        /// <summary>
        /// Set the WHERE clause of the query
        /// </summary>
        /// <param name="filter">
        /// An expression which denotes the where clause
        /// </param>
        IResultMapper<TResult> Where(Expression<Func<TResult, bool>> filter);
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IFilter<TArgs, TResult> : IResultMapper<TArgs, TResult>
    {
        /// <summary>
        /// Set the WHERE clause of the query
        /// </summary>
        /// <param name="filter">
        /// An expression which denotes the where clause
        /// </param>
        IResultMapper<TArgs, TResult> Where(Expression<Func<TResult, bool>> filter);
        
        /// <summary>
        /// Set the WHERE clause of the query
        /// </summary>
        /// <param name="filter">
        /// An expression which denotes the where clause
        /// </param>
        IResultMapper<TArgs, TResult> Where(Expression<Func<TResult, TArgs, bool>> filter);
    }
}
