using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Dsl
{
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface ITable<TResult>
    {
        /// <summary>
        /// Set the [Table] in SELECT FROM [Table]
        /// </summary>
        /// <param name="resultProperty">
        /// An expression to map the selected table to a property on the result
        /// </param>
        IQuery<TResult> From<TTable>(string tableName, Expression<Func<TResult, TTable>> resultProperty);
        

        //TODO: this method should be implemented
        // /// <summary>
        // /// Set the [Table] in SELECT FROM [Table]
        // /// </summary>
        // /// <param name="resultProperty">
        // /// An expression to map the selected table to a property on the result
        // /// </param>
        // IQuery<TResult> From<TTable>(string tableName, Expression<Func<TResult, IEnumerable<TTable>>> resultProperty);
        
        /// <summary>
        /// Set the [Table] in SELECT FROM [Table]. Uses the class name of TTable as the sql table name
        /// </summary>
        IQuery<TResult> From<TTable>(Expression<Func<TResult, TTable>> resultProperty);
        
        /// <summary>
        /// Set the [Table] in SELECT FROM [Table] to be TResult.
        /// </summary>
        IQuery<TResult> From(string tableName);
        
        /// <summary>
        /// Set the [Table] in SELECT FROM [Table] to be TResult. Uses the class name of TResult as the sql table name
        /// </summary>
        IQuery<TResult> From();
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface ITable<TArgs, TResult>
    {
        /// <summary>
        /// Set the [Table] in SELECT FROM [Table]
        /// </summary>
        /// <param name="resultProperty">
        /// An expression to map the selected table to a property on the result
        /// </param>
        IQuery<TArgs, TResult> From<TTable>(string tableName, Expression<Func<TResult, TTable>> resultProperty);
        

        //TODO: this method should be implemented
        // /// <summary>
        // /// Set the [Table] in SELECT FROM [Table]
        // /// </summary>
        // /// <param name="resultProperty">
        // /// An expression to map the selected table to a property on the result
        // /// </param>
        // IQuery<TResult> From<TTable>(string tableName, Expression<Func<TResult, IEnumerable<TTable>>> resultProperty);
        
        /// <summary>
        /// Set the [Table] in SELECT FROM [Table]. Uses the class name of TTable as the sql table name
        /// </summary>
        IQuery<TArgs, TResult> From<TTable>(Expression<Func<TResult, TTable>> resultProperty);
        
        /// <summary>
        /// Set the [Table] in SELECT FROM [Table] to be TResult.
        /// </summary>
        IQuery<TArgs, TResult> From(string tableName);
        
        /// <summary>
        /// Set the [Table] in SELECT FROM [Table] to be TResult. Uses the class name of TResult as the sql table name
        /// </summary>
        IQuery<TArgs, TResult> From();
    }
}
