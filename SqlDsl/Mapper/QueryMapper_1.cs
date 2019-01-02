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
    public class QueryMapper<TArgs, TResult, TMapped> : IPager<TArgs, TMapped>
    {
        readonly SqlExecutor<TArgs, TResult> Query;
        readonly Expression<Func<TResult, TArgs, TMapped>> Mapper;
        (int? skip, int? take) Paging;
        
        public QueryMapper(SqlExecutor<TArgs, TResult> query, Expression<Func<TResult, TArgs, TMapped>> mapper)
        {
            Query = query ?? throw new ArgumentNullException(nameof(query));
            Mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        }

        /// <inheritdoc />
        public ICompiledQuery<TArgs, TMapped> Compile(ILogger logger = null)
        {
            var timer = new Timer(true);
            var result = QueryMapper.Compile<TArgs, TResult, TMapped>(Query.SqlSyntax, Query, Mapper, logger: logger);

            if (logger.CanLogInfo(LogMessages.CompiledQuery))
                logger.LogInfo($"Query compiled in {timer.SplitString()}", LogMessages.CompiledQuery);

            return result;
        }

        /// <inheritdoc />
        public IPager2<TArgs, TMapped> Skip(int result)
        {
            Paging = (result, Paging.take);
            return this;
        }

        /// <inheritdoc />
        public ISqlExecutor<TArgs, TMapped> Take(int result)
        {
            Paging = (Paging.skip, result);
            return this;
        }

        public IPager2<TArgs, TMapped> Skip(Func<TArgs, int> result)
        {
            throw new NotImplementedException();
        }

        public ISqlExecutor<TArgs, TMapped> Take(Func<TArgs, int> result)
        {
            throw new NotImplementedException();
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
