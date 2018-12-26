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
    public abstract class Filter<TArgs, TResult> : Orderer<TArgs, TResult>, IFilter<TArgs, TResult>
    {
        protected override (ParameterExpression queryRoot, ParameterExpression args, Expression where)? WhereClause => _WhereClause;

        (ParameterExpression queryRoot, ParameterExpression args, Expression where)? _WhereClause;

        public Filter(ISqlSyntax sqlSyntax)
            : base(sqlSyntax)
        {
        }

        /// <inheritdoc/>
        public IResultMapper<TArgs, TResult> Where(Expression<Func<TResult, bool>> filter)
        {
            // create a new expression which is the same as the previous
            // but with 1 more (unused) arg
            var newExpr = Expression.Lambda<Func<TResult, TArgs, bool>>(
                filter.Body, filter.TailCall, filter.Parameters.Append(Expression.Parameter(typeof(TArgs))));

            return Where(newExpr);
        }

        /// <inheritdoc/>
        public IResultMapper<TArgs, TResult> Where(Expression<Func<TResult, TArgs, bool>> filter)
        {
            if (filter == null)
                throw new ArgumentNullException(nameof(filter));

            _WhereClause = (filter.Parameters[0], filter.Parameters[1], filter.Body);
            return this;
        }
    }
}