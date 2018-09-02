using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Dsl
{
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
    public interface IQuery<TArgs, TResult> : IFilter<TArgs, TResult>
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
        IJoinBuilder<TArgs, TResult, TJoin> InnerJoin<TJoin>(string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinProperty);
        
        /// <summary>
        /// Join another table to the query using INNER JOIN. Uses the name of the TJoin class as the join table name
        /// </summary>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TArgs, TResult, TJoin> InnerJoin<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinProperty);
        /// <summary>
        /// Join another table to the query using INNER JOIN
        /// </summary>
        /// <param name="tableName">
        /// The name of the table to join
        /// </param>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TArgs, TResult, TJoin> InnerJoin<TJoin>(string tableName, Expression<Func<TResult, TJoin>> joinProperty);
        
        /// <summary>
        /// Join another table to the query using INNER JOIN. Uses the name of the TJoin class as the join table name
        /// </summary>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TArgs, TResult, TJoin> InnerJoin<TJoin>(Expression<Func<TResult, TJoin>> joinProperty);
        
        /// <summary>
        /// Join another table to the query using LEFT JOIN
        /// </summary>
        /// <param name="tableName">
        /// The name of the table to join
        /// </param>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TArgs, TResult, TJoin> LeftJoin<TJoin>(string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinProperty);
        
        /// <summary>
        /// Join another table to the query using LEFT JOIN. Uses the name of the TJoin class as the join table name
        /// </summary>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TArgs, TResult, TJoin> LeftJoin<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinProperty);
        
        /// <summary>
        /// Join another table to the query using LEFT JOIN
        /// </summary>
        /// <param name="tableName">
        /// The name of the table to join
        /// </param>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TArgs, TResult, TJoin> LeftJoin<TJoin>(string tableName, Expression<Func<TResult, TJoin>> joinProperty);
        
        /// <summary>
        /// Join another table to the query using LEFT JOIN. Uses the name of the TJoin class as the join table name
        /// </summary>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        IJoinBuilder<TArgs, TResult, TJoin> LeftJoin<TJoin>(Expression<Func<TResult, TJoin>> joinProperty);
    }
}
