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
    /// <summary>
    /// Object to append query values to via underlying DSL
    /// </summary>
    public partial class QueryBuilder<TArgs, TResult>
    {
        /// <summary>
        /// Holds partial join state and can build a join
        /// </summary>
        class JoinBuilder<TJoin> : IJoinBuilder<TArgs, TResult, TJoin>
        {
            readonly QueryBuilder<TArgs, TResult> Query;
            readonly JoinType JoinType;
            readonly string TableName;
            readonly Expression JoinResultBody;
            readonly ParameterExpression JoinResultQueryParam;

            private JoinBuilder(
                QueryBuilder<TArgs, TResult> query, 
                JoinType joinType, 
                string tableName, 
                Expression joinResultBody, 
                ParameterExpression joinResultQueryParam)
            {
                Query = query ?? throw new ArgumentNullException(nameof(query));
                JoinType = joinType;
                JoinResultBody = joinResultBody ?? throw new ArgumentNullException(nameof(joinResultBody));
                JoinResultQueryParam = joinResultQueryParam ?? throw new ArgumentNullException(nameof(joinResultQueryParam));
                TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            }

            public JoinBuilder(QueryBuilder<TArgs, TResult> query, JoinType joinType, string tableName, Expression<Func<TResult, TJoin>> joinResult)
                : this(query, joinType, tableName, joinResult?.Body, joinResult?.Parameters[0])
            {
            }

            public JoinBuilder(QueryBuilder<TArgs, TResult> query, JoinType joinType, string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinResult)
                : this(query, joinType, tableName, joinResult?.Body, joinResult?.Parameters[0])
            {
            }
            
            /// <summary>
            /// Define how a JOIN table joins to other tables in the query
            /// </summary>
            /// <param name="joinExpression">
            /// An expression which describes the [ON] part of JOIN [Table] [ON]
            /// </param>
            public IQuery<TArgs, TResult> On(Expression<Func<TResult, TJoin, TArgs, bool>> joinExpression)
            {                    
                if (joinExpression == null)
                    throw new ArgumentNullException(nameof(joinExpression));

                Query.Joins.Add(new Join(
                    JoinType,
                    TableName,
                    (joinExpression.Parameters[0], joinExpression.Parameters[2], joinExpression.Parameters[1], joinExpression.Body),
                    CheckMemberExpression(JoinResultBody, JoinResultQueryParam)));

                return Query;
            }

            /// <summary>
            /// Define how a JOIN table joins to other tables in the query
            /// </summary>
            /// <param name="joinExpression">
            /// An expression which describes the [ON] part of JOIN [Table] [ON]
            /// </param>
            public IQuery<TArgs, TResult> On(Expression<Func<TResult, TJoin, bool>> joinExpression)
            {
                // create a new expression which is the same as the previous
                // but with 1 more (unused) arg
                var newExpr = Expression.Lambda<Func<TResult, TJoin, TArgs, bool>>(
                    joinExpression.Body, joinExpression.TailCall, joinExpression.Parameters.Append(Expression.Parameter(typeof(TArgs))));

                return On(newExpr);
            }
        }
    }
}
