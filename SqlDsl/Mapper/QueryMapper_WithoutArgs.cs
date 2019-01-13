using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Mapper
{
    /// <summary>
    /// Wrapper for ISqlBuilder&lt;object, TMapped> which ignores args
    /// </summary>
    public class QueryMapper<TMapped> : IPager<TMapped>
    {
        readonly IPager<object, TMapped> Worker;
        
        public QueryMapper(IPager<object, TMapped> worker)
        {
            Worker = worker ?? throw new ArgumentNullException(nameof(worker));
        }

        /// <inheritdoc />
        public IPager2<TMapped> Skip(int result)
        {
            Worker.Skip(result);
            return this;
        }

        /// <inheritdoc />
        public ISqlExecutor<TMapped> Take(int result)
        {
            Worker.Take(result);
            return this;
        }

        /// <inheritdoc />
        public ICompiledQuery<TMapped> Compile(ILogger logger = null)
        {
            return new CompiledQuery<TMapped>(Worker.Compile(logger: logger));
        }

        /// <inheritdoc />
        public Task<IEnumerable<TMapped>> ToIEnumerableAsync(IExecutor executor, ILogger logger = null)
        {
            return Worker.ToIEnumerableAsync(executor, null, logger: logger);
        }

        /// <inheritdoc />
        public IEnumerable<TMapped> ToIEnumerable(IExecutor executor, ILogger logger = null)
        {
            return Worker.ToIEnumerable(executor, null, logger: logger);
        }

        /// <inheritdoc />
        public Task<List<TMapped>> ToListAsync(IExecutor executor, ILogger logger = null)
        {
            return Worker.ToListAsync(executor, null, logger: logger);
        }

        /// <inheritdoc />
        public List<TMapped> ToList(IExecutor executor, ILogger logger = null)
        {
            return Worker.ToList(executor, null, logger: logger);
        }

        /// <inheritdoc />
        public Task<TMapped[]> ToArrayAsync(IExecutor executor, ILogger logger = null)
        {
            return Worker.ToArrayAsync(executor, null, logger: logger);
        }

        /// <inheritdoc />
        public TMapped[] ToArray(IExecutor executor, ILogger logger = null)
        {
            return Worker.ToArray(executor, null, logger: logger);
        }

        /// <inheritdoc />
        public Task<TMapped> FirstAsync(IExecutor executor, ILogger logger = null)
        {
            return Worker.FirstAsync(executor, null, logger: logger);
        }

        /// <inheritdoc />
        public TMapped First(IExecutor executor, ILogger logger = null)
        {
            return Worker.First(executor, null, logger: logger);
        }

        /// <inheritdoc />
        public Task<TMapped> FirstOrDefaultAsync(IExecutor executor, ILogger logger = null)
        {
            return Worker.FirstOrDefaultAsync(executor, null, logger: logger);
        }

        /// <inheritdoc />
        public TMapped FirstOrDefault(IExecutor executor, ILogger logger = null)
        {
            return Worker.FirstOrDefault(executor, null, logger: logger);
        }

        /// <inheritdoc />
        public Task<TMapped> SingleAsync(IExecutor executor, ILogger logger = null)
        {
            return Worker.SingleAsync(executor, null, logger: logger);
        }

        /// <inheritdoc />
        public TMapped Single(IExecutor executor, ILogger logger = null)
        {
            return Worker.Single(executor, null, logger: logger);
        }

        /// <inheritdoc />
        public Task<TMapped> SingleOrDefaultAsync(IExecutor executor, ILogger logger = null)
        {
            return Worker.SingleOrDefaultAsync(executor, null, logger: logger);
        }

        /// <inheritdoc />
        public TMapped SingleOrDefault(IExecutor executor, ILogger logger = null)
        {
            return Worker.SingleOrDefault(executor, null, logger: logger);
        }
    }
}
