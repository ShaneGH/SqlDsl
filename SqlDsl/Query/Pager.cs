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
    /// Map a query into something else
    /// </summary>
    public abstract class Pager<TArgs, TResult> : SqlExecutor<TArgs, TResult>, IPager<TArgs, TResult>
    {
        (Expression<Func<TArgs, int>> skip, Expression<Func<TArgs, int>> take) _Paging;
        protected override (Expression<Func<TArgs, int>> skip, Expression<Func<TArgs, int>> take) Paging => _Paging;

        public Pager(ISqlSyntax sqlSyntax)
            : base(sqlSyntax)
        {
        }

        /// <inheritdoc />
        public IPager2<TArgs, TResult> Skip(int result) => Skip(_ => result);

        /// <inheritdoc />
        public ISqlExecutor<TArgs, TResult> Take(int result) => Take(_ => result);

        /// <inheritdoc />
        public IPager2<TArgs, TResult> Skip(Expression<Func<TArgs, int>> result)
        {
            _Paging = (result, Paging.take);
            return this;
        }

        /// <inheritdoc />
        public ISqlExecutor<TArgs, TResult> Take(Expression<Func<TArgs, int>> result)
        {
            _Paging = (Paging.skip, result);
            return this;
        }
    }
}