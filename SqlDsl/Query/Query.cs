using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.Mapper;
using SqlDsl.Schema;
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
    /// Join other tables to a query
    /// </summary>
    public abstract class Query<TArgs, TResult> : Filter<TArgs, TResult>, IQuery<TArgs, TResult>
    {
        /// <inheritdoc />
        public override IEnumerable<Join> Joins => _Joins;
        
        /// <summary>
        /// The joins applied to the query
        /// </summary>
        readonly List<Join> _Joins = new List<Join>();

        public Query(ISqlSyntax sqlSyntax)
            : base(sqlSyntax)
        {
        }
        
        /// <inheritdoc />
        public IJoinBuilder<TArgs, TResult, TJoin> InnerJoinMany<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinResult) =>
            new JoinBuilder<TJoin>(this, JoinType.Inner, null, joinResult);
            
        /// <inheritdoc />
        IJoinBuilder<TArgs, TResult, TJoin> InnerJoin<TJoin>(string tableName, Expression<Func<TResult, TJoin>> joinResult) =>
            new JoinBuilder<TJoin>(this, JoinType.Inner, tableName, joinResult);
        
        /// <inheritdoc />
        public IJoinBuilder<TArgs, TResult, TJoin> InnerJoinOne<TJoin>(Expression<Func<TResult, TJoin>> joinResult) =>
            InnerJoin<TJoin>(null, joinResult);
        
        /// <inheritdoc />
        IJoinBuilder<TArgs, TResult, TJoin> LeftJoin<TJoin>(string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinResult) =>
            new JoinBuilder<TJoin>(this, JoinType.Left, tableName, joinResult);
        
        /// <inheritdoc />
        public IJoinBuilder<TArgs, TResult, TJoin> LeftJoinMany<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinResult) =>
            LeftJoin<TJoin>(null, joinResult);
        
        /// <inheritdoc />
        IJoinBuilder<TArgs, TResult, TJoin> LeftJoin<TJoin>(string tableName, Expression<Func<TResult, TJoin>> joinResult) =>
            new JoinBuilder<TJoin>(this, JoinType.Left, tableName, joinResult);
        
        /// <inheritdoc />
        public IJoinBuilder<TArgs, TResult, TJoin> LeftJoinOne<TJoin>(Expression<Func<TResult, TJoin>> joinResult) =>
            LeftJoin<TJoin>(null, joinResult);

        /// <summary>
        /// Holds partial join state and can build a join
        /// </summary>
        class JoinBuilder<TJoin> : IJoinBuilder<TArgs, TResult, TJoin>
        {
            readonly Query<TArgs, TResult> Query;
            readonly JoinType JoinType;
            readonly string TableName;
            readonly Expression JoinResultBody;
            readonly ParameterExpression JoinResultQueryParam;

            private JoinBuilder(
                Query<TArgs, TResult> query, 
                JoinType joinType, 
                string tableName, 
                Expression joinResultBody, 
                ParameterExpression joinResultQueryParam)
            {
                Query = query ?? throw new ArgumentNullException(nameof(query));
                JoinType = joinType;
                JoinResultBody = joinResultBody ?? throw new ArgumentNullException(nameof(joinResultBody));
                JoinResultQueryParam = joinResultQueryParam ?? throw new ArgumentNullException(nameof(joinResultQueryParam));
                
                var joinTableType = ReflectionUtils.GetIEnumerableType(typeof(TJoin)) ?? typeof(TJoin);
                TableName = tableName ?? TableAttribute.GetTableName(joinTableType);
            }

            public JoinBuilder(Query<TArgs, TResult> query, JoinType joinType, string tableName, Expression<Func<TResult, TJoin>> joinResult)
                : this(query, joinType, tableName, joinResult?.Body, joinResult?.Parameters[0])
            {
            }

            public JoinBuilder(Query<TArgs, TResult> query, JoinType joinType, string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinResult)
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

                Query._Joins.Add(new Join(
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

        /// <summary>
        /// Check an expression ultimately points to the query object. Throw an exception if not
        /// </summary>
        protected static (string name, Type Type) CheckMemberExpression(Expression body, ParameterExpression queryParameter)
        {
            body = ReflectionUtils.RemoveConvert(body);
            if (body == queryParameter)
                return (SqlStatementConstants.RootObjectAlias, queryParameter.Type);

            // build a chain of property names
            var output = new List<MemberInfo>();
            var expr = TryOne(body) as MemberExpression;
            while (expr != null)
            {
                output.Insert(0, expr.Member);
                expr = TryOne(expr.Expression) as MemberExpression;
            }

            if (!output.Any() || !output[0].DeclaringType.IsAssignableFrom(typeof(TResult)))
                throw new ArgumentException($"The expression \"{body}\" must point to a paramater on the query object.", nameof(body));
                
            var tableType = output.Last().GetPropertyOrFieldType();
            
            // return the name and type
            return (output.MemberName(), ReflectionUtils.GetIEnumerableType(tableType) ?? tableType);

            Expression TryOne(Expression val) => ReflectionUtils.IsOne(val) ?? val;
        }
    }
}