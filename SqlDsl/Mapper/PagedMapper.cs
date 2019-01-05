using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using SqlDsl.Dsl;
using SqlDsl.Query;

namespace SqlDsl.Mapper
{
    /// <summary>
    /// A wrapper for a mapper which can pass paging arguments to the underlying query if necessary
    /// </summary>
    public class PagedMapper<TArgs, TResult, TMapped> : IPager<TArgs, TMapped>
    {
        readonly QueryMapper<TArgs, TResult, TMapped> Mapper;
        readonly Pager<TArgs, TResult> Pager;

        public PagedMapper(QueryMapper<TArgs, TResult, TMapped> mapper, Pager<TArgs, TResult> pager)
        {
            Mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            Pager = pager ?? throw new ArgumentNullException(nameof(pager));
        }

        /// <inheritdoc />
        public ICompiledQuery<TArgs, TMapped> Compile(ILogger logger = null)
        {
            return Mapper.Compile(logger);
        }

        /// <inheritdoc />
        public TMapped First(IExecutor executor, TArgs args, ILogger logger = null)
        {
            return Mapper.First(executor, args, logger);
        }

        /// <inheritdoc />
        public Task<TMapped> FirstAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            return Mapper.FirstAsync(executor, args, logger);
        }

        /// <inheritdoc />
        public TMapped FirstOrDefault(IExecutor executor, TArgs args, ILogger logger = null)
        {
            return Mapper.FirstOrDefault(executor, args, logger);
        }

        /// <inheritdoc />
        public Task<TMapped> FirstOrDefaultAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            return Mapper.FirstOrDefaultAsync(executor, args, logger);
        }

        /// <inheritdoc />
        public TMapped Single(IExecutor executor, TArgs args, ILogger logger = null)
        {
            return Mapper.Single(executor, args, logger);
        }

        /// <inheritdoc />
        public Task<TMapped> SingleAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            return Mapper.SingleAsync(executor, args, logger);
        }

        /// <inheritdoc />
        public TMapped SingleOrDefault(IExecutor executor, TArgs args, ILogger logger = null)
        {
            return Mapper.SingleOrDefault(executor, args, logger);
        }

        /// <inheritdoc />
        public Task<TMapped> SingleOrDefaultAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            return Mapper.SingleOrDefaultAsync(executor, args, logger);
        }

        /// <inheritdoc />
        public IPager2<TArgs, TMapped> Skip(int result)
        {
            Pager.Skip(result);
            return this;
        }

        /// <inheritdoc />
        public IPager2<TArgs, TMapped> Skip(Expression<Func<TArgs, int>> result)
        {
            Pager.Skip(result);
            return this;
        }

        /// <inheritdoc />
        public ISqlExecutor<TArgs, TMapped> Take(int result)
        {
            Pager.Take(result);
            return this;
        }

        /// <inheritdoc />
        public ISqlExecutor<TArgs, TMapped> Take(Expression<Func<TArgs, int>> result)
        {
            Pager.Take(result);
            return this;
        }

        /// <inheritdoc />
        public TMapped[] ToArray(IExecutor executor, TArgs args, ILogger logger = null)
        {
            return Mapper.ToArray(executor, args, logger);
        }

        /// <inheritdoc />
        public Task<TMapped[]> ToArrayAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            return Mapper.ToArrayAsync(executor, args, logger);
        }

        /// <inheritdoc />
        public IEnumerable<TMapped> ToIEnumerable(IExecutor executor, TArgs args, ILogger logger = null)
        {
            return Mapper.ToIEnumerable(executor, args, logger);
        }

        /// <inheritdoc />
        public Task<IEnumerable<TMapped>> ToIEnumerableAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            return Mapper.ToIEnumerableAsync(executor, args, logger);
        }

        /// <inheritdoc />
        public List<TMapped> ToList(IExecutor executor, TArgs args, ILogger logger = null)
        {
            return Mapper.ToList(executor, args, logger);
        }

        /// <inheritdoc />
        public Task<List<TMapped>> ToListAsync(IExecutor executor, TArgs args, ILogger logger = null)
        {
            return Mapper.ToListAsync(executor, args, logger);
        }
    }
}