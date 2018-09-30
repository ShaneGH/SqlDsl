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
    public class QueryBuilder<TSqlBuilder, TResult> : QueryBuilder<TSqlBuilder, object, TResult>, ITable<TResult>, IQuery<TResult>
        where TSqlBuilder : ISqlFragmentBuilder, new()
    {
        // TODO: try to remove all casts from this class

        Task<IEnumerable<TResult>> ISqlBuilder<TResult>.ExecuteAsync(IExecutor executor) => ExecuteAsync(executor, null);

        ICompiledQuery<TResult> ISqlBuilder<TResult>.Compile() => new CompiledQuery<TResult>(base.Compile());

        IQuery<TResult> ITable<TResult>.From<TTable>(string tableName, Expression<Func<TResult, TTable>> resultProperty) =>
            (QueryBuilder<TSqlBuilder, TResult>)base.From(tableName, resultProperty);

        IQuery<TResult> ITable<TResult>.From<TTable>(Expression<Func<TResult, TTable>> resultProperty) =>
            (QueryBuilder<TSqlBuilder, TResult>)base.From(resultProperty);

        IQuery<TResult> ITable<TResult>.From(string tableName) =>
            (QueryBuilder<TSqlBuilder, TResult>)base.From(tableName);

        IQuery<TResult> ITable<TResult>.From() =>
            (QueryBuilder<TSqlBuilder, TResult>)base.From();

        IJoinBuilder<TResult, TJoin> IQuery<TResult>.InnerJoin<TJoin>(string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinProperty) =>
            new JoinBuilder_WithoutArgs<TJoin>(base.InnerJoin(tableName, joinProperty));

        IJoinBuilder<TResult, TJoin> IQuery<TResult>.InnerJoin<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinProperty) =>
            new JoinBuilder_WithoutArgs<TJoin>(base.InnerJoin(joinProperty));

        IJoinBuilder<TResult, TJoin> IQuery<TResult>.InnerJoin<TJoin>(string tableName, Expression<Func<TResult, TJoin>> joinProperty) =>
            new JoinBuilder_WithoutArgs<TJoin>(base.InnerJoin(tableName, joinProperty));

        IJoinBuilder<TResult, TJoin> IQuery<TResult>.InnerJoin<TJoin>(Expression<Func<TResult, TJoin>> joinProperty) =>
            new JoinBuilder_WithoutArgs<TJoin>(base.InnerJoin(joinProperty));

        IJoinBuilder<TResult, TJoin> IQuery<TResult>.LeftJoin<TJoin>(string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinProperty) =>
            new JoinBuilder_WithoutArgs<TJoin>(base.InnerJoin(tableName, joinProperty));

        IJoinBuilder<TResult, TJoin> IQuery<TResult>.LeftJoin<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinProperty) =>
            new JoinBuilder_WithoutArgs<TJoin>(base.InnerJoin(joinProperty));

        IJoinBuilder<TResult, TJoin> IQuery<TResult>.LeftJoin<TJoin>(string tableName, Expression<Func<TResult, TJoin>> joinProperty) =>
            new JoinBuilder_WithoutArgs<TJoin>(base.InnerJoin(tableName, joinProperty));

        IJoinBuilder<TResult, TJoin> IQuery<TResult>.LeftJoin<TJoin>(Expression<Func<TResult, TJoin>> joinProperty) =>
            new JoinBuilder_WithoutArgs<TJoin>(base.InnerJoin(joinProperty));

        ISqlBuilder<TMapped> IResultMapper<TResult>.Map<TMapped>(Expression<Func<TResult, TMapped>> mapper) =>
            new QueryMapper<TMapped>(base.Map(mapper));

        IResultMapper<TResult> IFilter<TResult>.Where(Expression<Func<TResult, bool>> filter) =>
            (QueryBuilder<TSqlBuilder, TResult>)base.Where(filter);
        
        /// <summary>
        /// Holds partial join state and can build a join
        /// </summary>
        class JoinBuilder_WithoutArgs<TJoin> : IJoinBuilder<TResult, TJoin>
        {
            readonly IJoinBuilder<object, TResult, TJoin> Worker;

            public JoinBuilder_WithoutArgs(IJoinBuilder<object, TResult, TJoin> worker)
            {
                Worker = worker ?? throw new ArgumentNullException(nameof(worker));
            }

            /// <summary>
            /// Define how a JOIN table joins to other tables in the query
            /// </summary>
            /// <param name="joinExpression">
            /// An expression which describes the [ON] part of JOIN [Table] [ON]
            /// </param>
            public IQuery<TResult> On(Expression<Func<TResult, TJoin, bool>> joinExpression)
            {
                return (QueryBuilder<TSqlBuilder, TResult>)Worker.On(joinExpression);
            }
        }
    }
}
