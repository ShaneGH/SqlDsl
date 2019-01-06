using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Dsl
{
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface ISqlSelect<TResult> : IFilter<TResult>
    {        
        /// <summary>
        /// Set the [Table] in SELECT FROM [Table]. Uses the class name of TTable as the sql table name
        /// </summary>
        IQuery<TResult> From<TTable>(Expression<Func<TResult, TTable>> resultProperty);
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface ISqlSelect<TArgs, TResult> : IFilter<TArgs, TResult>
    {        
        /// <summary>
        /// Set the [Table] in SELECT FROM [Table]. Uses the class name of TTable as the sql table name
        /// </summary>
        IQuery<TArgs, TResult> From<TTable>(Expression<Func<TResult, TTable>> resultProperty);
    }
}
