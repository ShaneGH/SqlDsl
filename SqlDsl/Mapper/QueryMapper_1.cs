using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using SqlDsl.Dsl;
using SqlDsl.Query;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    public class QueryMapper<TArgs, TResult, TMapped> : ISqlExecutor<TArgs, TMapped>
    {
        readonly SqlExecutor<TArgs, TResult> Query;
        readonly LambdaExpression Mapper;
        readonly bool RequiresPropertyUnwrap;
        
        public QueryMapper(SqlExecutor<TArgs, TResult> query, Expression<Func<TResult, TArgs, TMapped>> mapper)
        {
            Query = query ?? throw new ArgumentNullException(nameof(query));
            var (rpu, m) = EnsureComplexMap(mapper ?? throw new ArgumentNullException(nameof(mapper)));

            RequiresPropertyUnwrap = rpu;
            Mapper = m;
        }

        /// <summary>
        /// Ensure that the mapping statement begins with a new { } expression, as only these can be mapped
        /// <summary>
        static (bool requiresPropertyUnwrap, LambdaExpression expression) EnsureComplexMap(Expression<Func<TResult, TArgs, TMapped>> mapper)
        {
            var mapType = MapBuilder.GetMappingType(mapper.Body);
            if (mapType != MapBuilder.MappingType.SingleProp)
                return (false, mapper);
                
            var valType = typeof(PropMapValue<>).MakeGenericType(mapper.Body.Type);
            var body = Expression.MemberInit(
                Expression.New(
                    valType),
                Expression.Bind(
                    valType.GetField("Value"),
                    mapper.Body));

            return (
                true,
                Expression.Lambda<Func<TResult, TArgs, PropMapValue<TMapped>>>(
                    body,
                    mapper.Parameters));
        }

        /// <inheritdoc />
        public ICompiledQuery<TArgs, TMapped> Compile(ILogger logger = null)
        {
            var timer = new Timer(true);
            var result = QueryMapper.Compile<TArgs, TResult, TMapped>(Query.SqlSyntax, Query, Mapper, RequiresPropertyUnwrap, logger: logger);

            if (logger.CanLogInfo(LogMessages.CompiledQuery))
                logger.LogInfo($"Query compiled in {timer.SplitString()}", LogMessages.CompiledQuery);

            return result;
        }

        /// <inheritdoc />
        public TMapped[] ToArray(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).ToArray(executor, args, logger: logger);

        /// <inheritdoc />
        public Task<TMapped[]> ToArrayAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).ToArrayAsync(executor, args, logger: logger);

        /// <inheritdoc />
        public IEnumerable<TMapped> ToIEnumerable(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).ToIEnumerable(executor, args, logger: logger);

        /// <inheritdoc />
        public Task<IEnumerable<TMapped>> ToIEnumerableAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).ToIEnumerableAsync(executor, args, logger: logger);

        /// <inheritdoc />
        public List<TMapped> ToList(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).ToList(executor, args, logger: logger);

        /// <inheritdoc />
        public Task<List<TMapped>> ToListAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).ToListAsync(executor, args, logger: logger);

        /// <inheritdoc />
        public TMapped First(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).First(executor, args, logger: logger);

        /// <inheritdoc />
        public Task<TMapped> FirstAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).FirstAsync(executor, args, logger: logger);

        /// <inheritdoc />
        public TMapped FirstOrDefault(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).FirstOrDefault(executor, args, logger: logger);

        /// <inheritdoc />
        public Task<TMapped> FirstOrDefaultAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).FirstOrDefaultAsync(executor, args, logger: logger);

        /// <inheritdoc />
        public TMapped Single(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).Single(executor, args, logger: logger);

        /// <inheritdoc />
        public Task<TMapped> SingleAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).SingleAsync(executor, args, logger: logger);

        /// <inheritdoc />
        public TMapped SingleOrDefault(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).SingleOrDefault(executor, args, logger: logger);

        /// <inheritdoc />
        public Task<TMapped> SingleOrDefaultAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).SingleOrDefaultAsync(executor, args, logger: logger);
    }
}
