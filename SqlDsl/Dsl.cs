using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace SqlDsl
{    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface ITable<TResult>
        where TResult: new()
    {
        /// <summary>
        /// Set the [Table] in SELECT FROM [Table]
        /// </summary>
        /// <param name="resultProperty">
        /// An expression to map the selected table to a property on the result
        /// </param>
        IQuery<TResult> From<TTable>(string tableName, Expression<Func<TResult, TTable>> resultProperty);
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface ISqlBuilder<TResult>
        where TResult: new()
    {
        /// <summary>
        /// Get a sql statement and corresponding sql paramaters from the query
        /// </summary>
        (string sql, IEnumerable<object> paramaters) ToSql();
        
        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor);
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IResultMapper<TResult> : ISqlBuilder<TResult>
        where TResult: new()
    {
        /// <summary>
        /// Map the result TResult to another type of object. Use this method to cherry pick the columns you want to return
        /// </summary>
        /// <param name="mapper">
        /// An expression to build a mapped object
        /// </param>
        ISqlBuilder<TMapped> Map<TMapped>(Expression<Func<TResult, TMapped>> mapper) where TMapped: new();
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IFilter<TResult> : IResultMapper<TResult>
        where TResult: new()
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
    public interface IQuery<TResult> : IFilter<TResult>
        where TResult: new()
    {
        /// <summary>
        /// Join another table to the query using INNER JOIN
        /// </summary>
        /// <param name="tableName">
        /// The name of the table to join
        /// </param>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TResult, TJoin> InnerJoin<TJoin>(string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinProperty);
        
        /// <summary>
        /// Join another table to the query using LEFT JOIN
        /// </summary>
        /// <param name="tableName">
        /// The name of the table to join
        /// </param>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TResult, TJoin> LeftJoin<TJoin>(string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinProperty);
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IJoinBuilder<TResult, TJoin>
        where TResult: new()
    {
        /// <summary>
        /// Define how a JOIN table joins to other tables in the query
        /// </summary>
        /// <param name="joinExpression">
        /// An expression which describes the [ON] part of JOIN [Table] [ON]
        /// </param>
        IQuery<TResult> On(Expression<Func<TResult, TJoin, bool>> joinExpression);
    }
}
