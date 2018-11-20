using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace SqlDsl.Query
{
    public partial class QueryBuilder<TSqlBuilder, TArgs, TResult>
    {
        /// <summary>
        /// Set the [Table] in SELECT FROM [Table]
        /// </summary>
        /// <param name="resultProperty">
        /// An expression to map the selected table to a property on the result
        /// </param>
        public IQuery<TArgs, TResult> From<TTable>(string tableName, Expression<Func<TResult, TTable>> tableProperty)
        {
            PrimaryTableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            PrimaryTableMember = CheckMemberExpression(tableProperty.Body, tableProperty.Parameters[0]);

            return this;
        }

        /// <summary>
        /// Set the [Table] in SELECT FROM [Table]. Uses the class name of TTable as the sql table name
        /// </summary>
        public IQuery<TArgs, TResult> From<TTable>(Expression<Func<TResult, TTable>> tableProperty) =>
            From<TTable>(typeof(TTable).Name, tableProperty);

        /// <summary>
        /// Set the [Table] in SELECT FROM [Table] to be TResult.
        /// </summary>
        public IQuery<TArgs, TResult> From(string tableName) =>
            From<TResult>(tableName, x => x);

        /// <summary>
        /// Set the [Table] in SELECT FROM [Table] to be TResult. Uses the class name of TResult as the sql table name
        /// </summary>
        public IQuery<TArgs, TResult> From() =>
            From(typeof(TResult).Name);

        /// <summary>
        /// Join another table to the query using INNER JOIN
        /// </summary>
        /// <param name="tableName">
        /// The name of the table to join
        /// </param>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        public IJoinBuilder<TArgs, TResult, TJoin> InnerJoin<TJoin>(string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinResult) =>
            new JoinBuilder<TJoin>(this, JoinType.Inner, tableName, joinResult);
        
        /// <summary>
        /// Join another table to the query using INNER JOIN
        /// </summary>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        public IJoinBuilder<TArgs, TResult, TJoin> InnerJoin<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinResult) =>
            InnerJoin<TJoin>(typeof(TJoin).Name, joinResult);
            
        /// <summary>
        /// Join another table to the query using INNER JOIN
        /// </summary>
        /// <param name="tableName">
        /// The name of the table to join
        /// </param>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        public IJoinBuilder<TArgs, TResult, TJoin> InnerJoin<TJoin>(string tableName, Expression<Func<TResult, TJoin>> joinResult) =>
            new JoinBuilder<TJoin>(this, JoinType.Inner, tableName, joinResult);
        
        /// <summary>
        /// Join another table to the query using INNER JOIN
        /// </summary>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        public IJoinBuilder<TArgs, TResult, TJoin> InnerJoin<TJoin>(Expression<Func<TResult, TJoin>> joinResult) =>
            InnerJoin<TJoin>(typeof(TJoin).Name, joinResult);
        
        /// <summary>
        /// Join another table to the query using LEFT JOIN
        /// </summary>
        /// <param name="tableName">
        /// The name of the table to join
        /// </param>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        public IJoinBuilder<TArgs, TResult, TJoin> LeftJoin<TJoin>(string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinResult) =>
            new JoinBuilder<TJoin>(this, JoinType.Left, tableName, joinResult);
        
        /// <summary>
        /// Join another table to the query using LEFT JOIN
        /// </summary>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        public IJoinBuilder<TArgs, TResult, TJoin> LeftJoin<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinResult) =>
            LeftJoin<TJoin>(typeof(TJoin).Name, joinResult);
        
        /// <summary>
        /// Join another table to the query using LEFT JOIN
        /// </summary>
        /// <param name="tableName">
        /// The name of the table to join
        /// </param>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        public IJoinBuilder<TArgs, TResult, TJoin> LeftJoin<TJoin>(string tableName, Expression<Func<TResult, TJoin>> joinResult) =>
            new JoinBuilder<TJoin>(this, JoinType.Left, tableName, joinResult);
        
        /// <summary>
        /// Join another table to the query using LEFT JOIN
        /// </summary>
        /// <param name="joinProperty">
        /// The property of TResult to append the joined table to
        /// </param>
        public IJoinBuilder<TArgs, TResult, TJoin> LeftJoin<TJoin>(Expression<Func<TResult, TJoin>> joinResult) =>
            LeftJoin<TJoin>(typeof(TJoin).Name, joinResult);

        /// <summary>
        /// Set the WHERE clause of the query
        /// </summary>
        /// <param name="filter">
        /// An expression which denotes the where clause
        /// </param>
        public IResultMapper<TArgs, TResult> Where(Expression<Func<TResult, bool>> filter)
        {
            // create a new expression which is the same as the previous
            // but with 1 more (unused) arg
            var newExpr = Expression.Lambda<Func<TResult, TArgs, bool>>(
                filter.Body, filter.TailCall, filter.Parameters.Append(Expression.Parameter(typeof(TArgs))));

            return Where(newExpr);
        }

        public IResultMapper<TArgs, TResult> Where(Expression<Func<TResult, TArgs, bool>> filter)
        {
            if (filter == null)
                throw new ArgumentNullException(nameof(filter));

            WhereClause = (filter.Parameters[0], filter.Parameters[1], filter.Body);
            return this;
        }

        /// <summary>
        /// Map the result TResult to another type of object. Use this method to cherry pick the columns you want to return
        /// </summary>
        /// <param name="mapper">
        /// An expression to build a mapped object
        /// </param>
        public ISqlBuilder<TArgs, TMapped> Map<TMapped>(Expression<Func<TResult, TArgs, TMapped>> mapper) =>
            new QueryMapper<TSqlBuilder, TArgs, TResult, TMapped>(this, mapper);

        /// <summary>
        /// Map the result TResult to another type of object. Use this method to cherry pick the columns you want to return
        /// </summary>
        /// <param name="mapper">
        /// An expression to build a mapped object
        /// </param>
        public ISqlBuilder<TArgs, TMapped> Map<TMapped>(Expression<Func<TResult, TMapped>> mapper)
        {
            var addedArgs = Expression.Lambda<Func<TResult, TArgs, TMapped>>(
                mapper.Body,
                mapper.Parameters[0],
                Expression.Parameter(typeof(TArgs)));

            return Map(addedArgs);
        }

        private readonly List<(Expression orderExpression, OrderDirection direction)> Ordering = new List<(Expression, OrderDirection)>();

        /// <inheritdoc />
        public IOrdererAgain<TArgs, TResult> OrderBy<T>(Expression<Func<TResult, T>> order)
        {
            Ordering.Add((order.Body, OrderDirection.Ascending));
            return this;
        }
        
        /// <inheritdoc />
        public IOrdererAgain<TArgs, TResult> OrderByDesc<T>(Expression<Func<TResult, T>> order)
        {
            Ordering.Add((order.Body, OrderDirection.Descending));
            return this;
        }

        /// <inheritdoc />
        public IOrdererAgain<TArgs, TResult> ThenBy<T>(Expression<Func<TResult, T>> order) => OrderBy(order);
        
        /// <inheritdoc />
        public IOrdererAgain<TArgs, TResult> ThenByDesc<T>(Expression<Func<TResult, T>> order) => OrderByDesc(order);
    }
}