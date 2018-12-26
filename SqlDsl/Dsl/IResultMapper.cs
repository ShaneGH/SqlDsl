using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Dsl
{    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IResultMapper<TResult> : ISqlExecutor<TResult>, IOrderer<TResult>
    {
        /// <summary>
        /// Map the result TResult to another type of object. Use this method to cherry pick the columns you want to return
        /// </summary>
        /// <param name="mapper">
        /// An expression to build a mapped object
        /// </param>
        ISqlExecutor<TMapped> Map<TMapped>(Expression<Func<TResult, TMapped>> mapper);
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IResultMapper<TArgs, TResult> : ISqlExecutor<TArgs, TResult>, IOrderer<TArgs, TResult>
    {
        /// <summary>
        /// Map the result TResult to another type of object. Use this method to cherry pick the columns you want to return
        /// </summary>
        /// <param name="mapper">
        /// An expression to build a mapped object
        /// </param>
        ISqlExecutor<TArgs, TMapped> Map<TMapped>(Expression<Func<TResult, TMapped>> mapper);
        
        /// <summary>
        /// Map the result TResult to another type of object. Use this method to cherry pick the columns you want to return
        /// </summary>
        /// <param name="mapper">
        /// An expression to build a mapped object
        /// </param>
        ISqlExecutor<TArgs, TMapped> Map<TMapped>(Expression<Func<TResult, TArgs, TMapped>> mapper);
    }
}
