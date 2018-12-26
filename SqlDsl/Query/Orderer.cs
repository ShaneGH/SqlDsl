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
    public abstract class Orderer<TArgs, TResult> : ResultMapper<TArgs, TResult>, IOrderer<TArgs, TResult>, IOrdererAgain<TArgs, TResult>
    {        
        protected override IEnumerable<(ParameterExpression queryRoot, ParameterExpression args, Expression orderExpression, OrderDirection direction)> Ordering => _Ordering;
        
        readonly List<(ParameterExpression queryRoot, ParameterExpression args, Expression orderExpression, OrderDirection direction)> _Ordering = new List<(ParameterExpression, ParameterExpression, Expression, OrderDirection)>();

        public Orderer(ISqlSyntax sqlSyntax)
            : base(sqlSyntax)
        {
        }

        /// <inheritdoc />
        public override IOrdererAgain<TArgs, TResult> OrderBy<T>(Expression<Func<TResult, TArgs, T>> order)
        {
            _Ordering.Add((order.Parameters[0], order.Parameters[1], order.Body, OrderDirection.Ascending));
            return this;
        }
        
        /// <inheritdoc />
        public override IOrdererAgain<TArgs, TResult> OrderByDesc<T>(Expression<Func<TResult, TArgs, T>> order)
        {
            _Ordering.Add((order.Parameters[0], order.Parameters[1], order.Body, OrderDirection.Descending));
            return this;
        }

        /// <inheritdoc />
        public override IOrdererAgain<TArgs, TResult> OrderBy<T>(Expression<Func<TResult, T>> order)
        {
            _Ordering.Add((order.Parameters[0], null, order.Body, OrderDirection.Ascending));
            return this;
        }
        
        /// <inheritdoc />
        public override IOrdererAgain<TArgs, TResult> OrderByDesc<T>(Expression<Func<TResult, T>> order)
        {
            _Ordering.Add((order.Parameters[0], null, order.Body, OrderDirection.Descending));
            return this;
        }

        /// <inheritdoc />
        public IOrdererAgain<TArgs, TResult> ThenBy<T>(Expression<Func<TResult, T>> order) => OrderBy(order);
        
        /// <inheritdoc />
        public IOrdererAgain<TArgs, TResult> ThenByDesc<T>(Expression<Func<TResult, T>> order) => OrderByDesc(order);

        /// <inheritdoc />
        public IOrdererAgain<TArgs, TResult> ThenBy<T>(Expression<Func<TResult, TArgs, T>> order) => OrderBy(order);
        
        /// <inheritdoc />
        public IOrdererAgain<TArgs, TResult> ThenByDesc<T>(Expression<Func<TResult, TArgs, T>> order) => OrderByDesc(order);
    }
}