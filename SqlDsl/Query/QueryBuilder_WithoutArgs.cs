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
    public class QueryBuilder<TSqlBuilder, TResult> : QueryBuilder<TSqlBuilder, object, TResult>, ITable<TResult>, IQuery<TResult>, IOrdererAgain<TResult>
        where TSqlBuilder : ISqlSyntax, new()
    {
        Task<IEnumerable<TResult>> ISqlBuilder<TResult>.ToIEnumerableAsync(IExecutor executor, ILogger logger) => ToIEnumerableAsync(executor, null, logger: logger);

        IEnumerable<TResult> ISqlBuilder<TResult>.ToIEnumerable(IExecutor executor, ILogger logger) => ToIEnumerable(executor, null, logger: logger);

        Task<List<TResult>> ISqlBuilder<TResult>.ToListAsync(IExecutor executor, ILogger logger) => ToListAsync(executor, null, logger: logger);

        List<TResult> ISqlBuilder<TResult>.ToList(IExecutor executor, ILogger logger) => ToList(executor, null, logger: logger);

        Task<TResult[]> ISqlBuilder<TResult>.ToArrayAsync(IExecutor executor, ILogger logger) => ToArrayAsync(executor, null, logger: logger);

        TResult[] ISqlBuilder<TResult>.ToArray(IExecutor executor, ILogger logger) => ToArray(executor, null, logger: logger);

        ICompiledQuery<TResult> ISqlBuilder<TResult>.Compile(ILogger logger) => new CompiledQuery<TResult>(base.Compile(logger: logger));

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

        /// <inheritdoc />
        IOrdererAgain<TResult> IOrderer<TResult>.OrderBy<T>(Expression<Func<TResult, T>> order) => 
            (QueryBuilder<TSqlBuilder, TResult>)base.OrderBy(order);

        /// <inheritdoc />
        IOrdererAgain<TResult> IOrderer<TResult>.OrderByDesc<T>(Expression<Func<TResult, T>> order) => 
            (QueryBuilder<TSqlBuilder, TResult>)base.OrderByDesc(order);

        /// <inheritdoc />
        IOrdererAgain<TResult> IOrdererAgain<TResult>.ThenBy<T>(Expression<Func<TResult, T>> order) => 
            (QueryBuilder<TSqlBuilder, TResult>)base.ThenBy(order);

        /// <inheritdoc />
        IOrdererAgain<TResult> IOrdererAgain<TResult>.ThenByDesc<T>(Expression<Func<TResult, T>> order) => 
            (QueryBuilder<TSqlBuilder, TResult>)base.ThenByDesc(order);

        public Task<List<TResult>> ToListAsync(IExecutor executor, ILogger logger = null) =>
            base.ToListAsync(executor, null, logger);

        public List<TResult> ToList(IExecutor executor, ILogger logger = null) =>
            base.ToList(executor, null, logger);

        public Task<TResult[]> ToArrayAsync(IExecutor executor, ILogger logger = null) =>
            base.ToArrayAsync(executor, null, logger);

        public TResult[] ToArray(IExecutor executor, ILogger logger = null) =>
            base.ToArray(executor, null, logger);

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
