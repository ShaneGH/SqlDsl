using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl
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
    public interface ISqlBuilder<TResult>
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
    {
        /// <summary>
        /// Map the result TResult to another type of object. Use this method to cherry pick the columns you want to return
        /// </summary>
        /// <param name="mapper">
        /// An expression to build a mapped object
        /// </param>
        ISqlBuilder<TMapped> Map<TMapped>(Expression<Func<TResult, TMapped>> mapper);
    }
    
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
    public interface IQuery<TResult> : IFilter<TResult>
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
        /// Join another table to the query using INNER JOIN. Uses the name of the TJoin class as the join table name
        /// </summary>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TResult, TJoin> InnerJoin<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinProperty);
        /// <summary>
        /// Join another table to the query using INNER JOIN
        /// </summary>
        /// <param name="tableName">
        /// The name of the table to join
        /// </param>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TResult, TJoin> InnerJoin<TJoin>(string tableName, Expression<Func<TResult, TJoin>> joinProperty);
        
        /// <summary>
        /// Join another table to the query using INNER JOIN. Uses the name of the TJoin class as the join table name
        /// </summary>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TResult, TJoin> InnerJoin<TJoin>(Expression<Func<TResult, TJoin>> joinProperty);
        
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
        
        /// <summary>
        /// Join another table to the query using LEFT JOIN. Uses the name of the TJoin class as the join table name
        /// </summary>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TResult, TJoin> LeftJoin<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinProperty);
        
        /// <summary>
        /// Join another table to the query using LEFT JOIN
        /// </summary>
        /// <param name="tableName">
        /// The name of the table to join
        /// </param>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TResult, TJoin> LeftJoin<TJoin>(string tableName, Expression<Func<TResult, TJoin>> joinProperty);
        
        /// <summary>
        /// Join another table to the query using LEFT JOIN. Uses the name of the TJoin class as the join table name
        /// </summary>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TResult, TJoin> LeftJoin<TJoin>(Expression<Func<TResult, TJoin>> joinProperty);
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IJoinBuilder<TResult, TJoin>
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
