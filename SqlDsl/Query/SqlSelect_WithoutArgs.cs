using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.Mapper;
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
    /// Class to kick off a sql statement
    /// </summary>
    public class SqlSelect<TResult> : SqlSelect<object, TResult>, ISqlSelect<TResult>, IQuery<TResult>, IOrdererAgain<TResult>
    {
        /// <param name="strictJoins">If set to true, every join added to the SqlDsl query will also be added to the Sql query.
        /// If false, joins which are not used in a mapping, WHERE clause, ON clause etc... will be automatically removed</param>
        public SqlSelect(ISqlSyntax syntax, bool strictJoins)
            : base(syntax, strictJoins)
        {
        }

        IPager2<TResult> IPager<TResult>.Skip(int result) => (SqlSelect<TResult>)base.Skip(result);

        ISqlExecutor<TResult> IPager2<TResult>.Take(int result) => (SqlSelect<TResult>)base.Take(result);

        Task<IEnumerable<TResult>> ICompiledQuery<TResult>.ToIEnumerableAsync(IExecutor executor, ILogger logger) => ToIEnumerableAsync(executor, null, logger: logger);

        IEnumerable<TResult> ICompiledQuery<TResult>.ToIEnumerable(IExecutor executor, ILogger logger) => ToIEnumerable(executor, null, logger: logger);

        Task<List<TResult>> ICompiledQuery<TResult>.ToListAsync(IExecutor executor, ILogger logger) => ToListAsync(executor, null, logger: logger);

        List<TResult> ICompiledQuery<TResult>.ToList(IExecutor executor, ILogger logger) => ToList(executor, null, logger: logger);

        Task<TResult[]> ICompiledQuery<TResult>.ToArrayAsync(IExecutor executor, ILogger logger) => ToArrayAsync(executor, null, logger: logger);

        TResult[] ICompiledQuery<TResult>.ToArray(IExecutor executor, ILogger logger) => ToArray(executor, null, logger: logger);

        Task<TResult> ICompiledQuery<TResult>.FirstAsync(IExecutor executor, ILogger logger) => FirstAsync(executor, null, logger: logger);

        TResult ICompiledQuery<TResult>.First(IExecutor executor, ILogger logger) => First(executor, null, logger: logger);

        Task<TResult> ICompiledQuery<TResult>.FirstOrDefaultAsync(IExecutor executor, ILogger logger) => FirstOrDefaultAsync(executor, null, logger: logger);

        TResult ICompiledQuery<TResult>.FirstOrDefault(IExecutor executor, ILogger logger) => FirstOrDefault(executor, null, logger: logger);

        Task<TResult> ICompiledQuery<TResult>.SingleAsync(IExecutor executor, ILogger logger) => SingleAsync(executor, null, logger: logger);

        TResult ICompiledQuery<TResult>.Single(IExecutor executor, ILogger logger) => Single(executor, null, logger: logger);

        Task<TResult> ICompiledQuery<TResult>.SingleOrDefaultAsync(IExecutor executor, ILogger logger) => SingleOrDefaultAsync(executor, null, logger: logger);

        TResult ICompiledQuery<TResult>.SingleOrDefault(IExecutor executor, ILogger logger) => SingleOrDefault(executor, null, logger: logger);

        ICompiledQuery<TResult> ISqlExecutor<TResult>.Compile(ILogger logger) => new CompiledQuery<TResult>(base.Compile(logger: logger));

        IQuery<TResult> ISqlSelect<TResult>.From<TTable>(Expression<Func<TResult, TTable>> resultProperty) =>
            (SqlSelect<TResult>)base.From(resultProperty);

        IJoinBuilder<TResult, TJoin> IQuery<TResult>.InnerJoinMany<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinProperty) =>
            new JoinBuilder_WithoutArgs<TJoin>(base.InnerJoinMany(joinProperty));

        IJoinBuilder<TResult, TJoin> IQuery<TResult>.InnerJoinOne<TJoin>(Expression<Func<TResult, TJoin>> joinProperty) =>
            new JoinBuilder_WithoutArgs<TJoin>(base.InnerJoinOne(joinProperty));

        IJoinBuilder<TResult, TJoin> IQuery<TResult>.LeftJoinMany<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinProperty) =>
            new JoinBuilder_WithoutArgs<TJoin>(base.LeftJoinMany(joinProperty));

        IJoinBuilder<TResult, TJoin> IQuery<TResult>.LeftJoinOne<TJoin>(Expression<Func<TResult, TJoin>> joinProperty) =>
            new JoinBuilder_WithoutArgs<TJoin>(base.LeftJoinOne(joinProperty));

        IPager<TMapped> IResultMapper<TResult>.Map<TMapped>(Expression<Func<TResult, TMapped>> mapper) =>
            new QueryMapper<TMapped>(base.Map(mapper));

        IResultMapper<TResult> IFilter<TResult>.Where(Expression<Func<TResult, bool>> filter) =>
            (SqlSelect<TResult>)base.Where(filter);

        /// <inheritdoc />
        IOrdererAgain<TResult> IOrderer<TResult>.OrderBy<T>(Expression<Func<TResult, T>> order) => 
            (SqlSelect<TResult>)base.OrderBy(order);

        /// <inheritdoc />
        IOrdererAgain<TResult> IOrderer<TResult>.OrderByDesc<T>(Expression<Func<TResult, T>> order) => 
            (SqlSelect<TResult>)base.OrderByDesc(order);

        /// <inheritdoc />
        IOrdererAgain<TResult> IOrdererAgain<TResult>.ThenBy<T>(Expression<Func<TResult, T>> order) => 
            (SqlSelect<TResult>)base.ThenBy(order);

        /// <inheritdoc />
        IOrdererAgain<TResult> IOrdererAgain<TResult>.ThenByDesc<T>(Expression<Func<TResult, T>> order) => 
            (SqlSelect<TResult>)base.ThenByDesc(order);

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
                return (SqlSelect<TResult>)Worker.On(joinExpression);
            }
        }
    }
}
