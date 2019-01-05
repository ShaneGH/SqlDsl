using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Dsl
{
    /// <summary>
    /// Allows specifying paing on a query
    /// </summary>
    public interface IPager<TResult> : IPager2<TResult>
    {
        /// <summary>
        /// Skip the specified number of results
        /// </summary>
        IPager2<TResult> Skip(int result);
    }
    
    /// <summary>
    /// Allows specifying paing on a query
    /// </summary>
    public interface IPager2<TResult> : ISqlExecutor<TResult>
    {
        /// <summary>
        /// Take the specified number of results
        /// </summary>
        ISqlExecutor<TResult> Take(int result);
    }
    
    /// <summary>
    /// Allows specifying paing on a query
    /// </summary>
    public interface IPager<TArgs, TResult> : IPager2<TArgs, TResult>
    {
        /// <summary>
        /// Skip the specified number of results
        /// </summary>
        IPager2<TArgs, TResult> Skip(int result);
        
        /// <summary>
        /// Skip the specified number of results
        /// </summary>
        IPager2<TArgs, TResult> Skip(Expression<Func<TArgs, int>> result);
    }
    
    /// <summary>
    /// Allows specifying paing on a query
    /// </summary>
    public interface IPager2<TArgs, TResult> : ISqlExecutor<TArgs, TResult>
    {
        /// <summary>
        /// Take the specified number of results
        /// </summary>
        ISqlExecutor<TArgs, TResult> Take(int result);
        
        /// <summary>
        /// Take the specified number of results
        /// </summary>
        ISqlExecutor<TArgs, TResult> Take(Expression<Func<TArgs, int>> result);
    }
}
