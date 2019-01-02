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
        (int? skip, int? take) _Paging;
        protected override (int? skip, int? take) Paging => _Paging;

        public Pager(ISqlSyntax sqlSyntax)
            : base(sqlSyntax)
        {
        }

        public IPager2<TArgs, TResult> Skip(int result)
        {
            _Paging = (result, Paging.take);
            return this;
        }

        public ISqlExecutor<TArgs, TResult> Take(int result)
        {
            _Paging = (Paging.skip, result);
            return this;
        }

        public IPager2<TArgs, TResult> Skip(Func<TArgs, int> result)
        {
            throw new NotImplementedException();
        }

        public ISqlExecutor<TArgs, TResult> Take(Func<TArgs, int> result)
        {
            throw new NotImplementedException();
        }
    }
}