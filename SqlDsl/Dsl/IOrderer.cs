using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Dsl
{
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IOrderer<TResult>
    {
        /// <summary>
        /// Order by a property (descending)
        /// </summary>
        IOrdererAgain<TResult> OrderBy<T>(Expression<Func<TResult, T>> order);
        
        /// <summary>
        /// Order by a property (ascending)
        /// </summary>
        IOrdererAgain<TResult> OrderByDesc<T>(Expression<Func<TResult, T>> order);
    }

    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IOrdererAgain<TResult> : IResultMapper<TResult>
    {
        /// <summary>
        /// Order by a property (ascending)
        /// </summary>
        IOrdererAgain<TResult> ThenBy<T>(Expression<Func<TResult, T>> order);
        
        /// <summary>
        /// Order by a property (descending)
        /// </summary>
        IOrdererAgain<TResult> ThenByDesc<T>(Expression<Func<TResult, T>> order);
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IOrderer<TArgs, TResult>
    {
        /// <summary>
        /// Order by a property (ascending)
        /// </summary>
        IOrdererAgain<TArgs, TResult> OrderBy<T>(Expression<Func<TResult, T>> order);
        
        /// <summary>
        /// Order by a property (descending)
        /// </summary>
        IOrdererAgain<TArgs, TResult> OrderByDesc<T>(Expression<Func<TResult, T>> order);
    }

    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IOrdererAgain<TArgs, TResult> : IResultMapper<TArgs, TResult>
    {
        /// <summary>
        /// Order by a property (ascending)
        /// </summary>
        IOrdererAgain<TArgs, TResult> ThenBy<T>(Expression<Func<TResult, T>> order);
        
        /// <summary>
        /// Order by a property (descending)
        /// </summary>
        IOrdererAgain<TArgs, TResult> ThenByDesc<T>(Expression<Func<TResult, T>> order);
    }
}
