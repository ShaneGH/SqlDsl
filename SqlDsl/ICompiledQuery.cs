using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using SqlDsl.DataParser;
using SqlDsl.Query;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl
{
    /// <summary>
    /// A query compiled for reuse
    /// </summary>
    public interface ICompiledQuery<TResult>
    {
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<IEnumerable<TResult>> ToIEnumerableAsync(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        IEnumerable<TResult> ToIEnumerable(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<List<TResult>> ToListAsync(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        List<TResult> ToList(IExecutor executor, ILogger logger = null);
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<TResult[]> ToArrayAsync(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        TResult[] ToArray(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        TResult First(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<TResult> FirstAsync(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        TResult FirstOrDefault(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<TResult> FirstOrDefaultAsync(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        TResult Single(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<TResult> SingleAsync(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        TResult SingleOrDefault(IExecutor executor, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<TResult> SingleOrDefaultAsync(IExecutor executor, ILogger logger = null);
    }

    /// <summary>
    /// A query compiled for reuse
    /// </summary>
    public interface ICompiledQuery<TArgs, TResult>
    {
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<IEnumerable<TResult>> ToIEnumerableAsync(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        IEnumerable<TResult> ToIEnumerable(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<List<TResult>> ToListAsync(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        List<TResult> ToList(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<TResult[]> ToArrayAsync(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        TResult[] ToArray(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        TResult First(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<TResult> FirstAsync(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        TResult FirstOrDefault(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<TResult> FirstOrDefaultAsync(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        TResult Single(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<TResult> SingleAsync(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        TResult SingleOrDefault(IExecutor executor, TArgs args, ILogger logger = null);
        
        /// <summary>
        /// Execute the compiled query
        /// </summary>
        Task<TResult> SingleOrDefaultAsync(IExecutor executor, TArgs args, ILogger logger = null);
    }
}
