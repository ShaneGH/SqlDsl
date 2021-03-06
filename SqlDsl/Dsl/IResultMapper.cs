using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Dsl
{    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IResultMapper<TResult> : IPager<TResult>, IOrderer<TResult>
    {
        /// <summary>
        /// Map the result TResult to another type of object. Use this method to cherry pick the columns you want to return
        /// </summary>
        /// <param name="mapper">
        /// An expression to build a mapped object
        /// </param>
        IPager<TMapped> Map<TMapped>(Expression<Func<TResult, TMapped>> mapper);
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IResultMapper<TArgs, TResult> : IPager<TArgs, TResult>, IOrderer<TArgs, TResult>
    {
        /// <summary>
        /// Map the result TResult to another type of object. Use this method to cherry pick the columns you want to return
        /// </summary>
        /// <param name="mapper">
        /// An expression to build a mapped object
        /// </param>
        IPager<TArgs, TMapped> Map<TMapped>(Expression<Func<TResult, TMapped>> mapper);
        
        /// <summary>
        /// Map the result TResult to another type of object. Use this method to cherry pick the columns you want to return
        /// </summary>
        /// <param name="mapper">
        /// An expression to build a mapped object
        /// </param>
        IPager<TArgs, TMapped> Map<TMapped>(Expression<Func<TResult, TArgs, TMapped>> mapper);
    }
}
