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
    public abstract class ResultMapper<TArgs, TResult> : Pager<TArgs, TResult>, IResultMapper<TArgs, TResult>
    {
        public ResultMapper(ISqlSyntax sqlSyntax)
            : base(sqlSyntax)
        {
        }

        /// <inheritdoc />
        public IPager<TArgs, TMapped> Map<TMapped>(Expression<Func<TResult, TArgs, TMapped>> mapper) =>
            new PagedMapper<TArgs, TResult, TMapped>(
                new QueryMapper<TArgs, TResult, TMapped>(this, mapper),
                this);

        /// <inheritdoc />
        public IPager<TArgs, TMapped> Map<TMapped>(Expression<Func<TResult, TMapped>> mapper)
        {
            var addedArgs = Expression.Lambda<Func<TResult, TArgs, TMapped>>(
                mapper.Body,
                mapper.Parameters[0],
                Expression.Parameter(typeof(TArgs)));

            return Map(addedArgs);
        }

        public abstract IOrdererAgain<TArgs, TResult> OrderBy<T>(Expression<Func<TResult, T>> order);

        public abstract IOrdererAgain<TArgs, TResult> OrderBy<T>(Expression<Func<TResult, TArgs, T>> order);

        public abstract IOrdererAgain<TArgs, TResult> OrderByDesc<T>(Expression<Func<TResult, T>> order);

        public abstract IOrdererAgain<TArgs, TResult> OrderByDesc<T>(Expression<Func<TResult, TArgs, T>> order);
    }
}