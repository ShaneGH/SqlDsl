using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Dsl
{    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface ISqlExecutor<TResult> : ICompiledQuery<TResult>
    {        
        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        /// <param name="logger">A logger to capture info and warning messages</param>
        ICompiledQuery<TResult> Compile(ILogger logger = null);
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface ISqlExecutor<TArgs, TResult> : ICompiledQuery<TArgs, TResult>
    {   
        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        /// <param name="logger">A logger to capture info and warning messages</param>
        ICompiledQuery<TArgs, TResult> Compile(ILogger logger = null);
    }
}
