using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Query
{
    /// <summary>
    /// Wrapper for ISqlBuilder&lt;object, TMapped> which ignores args
    /// </summary>
    public class QueryMapper<TMapped> : ISqlBuilder<TMapped>
    {
        readonly ISqlBuilder<object, TMapped> Worker;
        
        public QueryMapper(ISqlBuilder<object, TMapped> worker)
        {
            Worker = worker ?? throw new ArgumentNullException(nameof(worker));
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
    }
}
