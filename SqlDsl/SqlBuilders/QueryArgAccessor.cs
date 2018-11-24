using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    static class QueryArgAccessor
    {
        static readonly ConcurrentDictionary<Type, Func<ParameterExpression, Expression, object>> Builders = 
            new ConcurrentDictionary<Type, Func<ParameterExpression, Expression, object>>();
        static readonly ConcurrentDictionary<Type, Func<object>> IdentityBuilders = 
            new ConcurrentDictionary<Type, Func<object>>();

        public static object Create(ParameterExpression parameter)
        {
            if (!IdentityBuilders.TryGetValue(parameter.Type, out Func<object> builder))
            {
                var constructor = typeof(QueryArgAccessor<>)
                    .MakeGenericType(parameter.Type)
                    .GetConstructor(new Type[0]);

                builder = IdentityBuilders.GetOrAdd(
                    parameter.Type,
                    Expression
                        .Lambda<Func<object>>(
                            ReflectionUtils.Convert(
                                Expression.New(constructor),
                                typeof(object)))
                        .Compile());
            }
            
            return builder();
        }

        public static object Create(ParameterExpression parameter, Expression accessor)
        {
            if (!Builders.TryGetValue(parameter.Type, out Func<ParameterExpression, Expression, object> builder))
            {
                var constructor = typeof(QueryArgAccessor<>)
                    .MakeGenericType(parameter.Type)
                    .GetConstructor(new[]{ typeof(ParameterExpression), typeof(Expression) });

                var param1 = Expression.Parameter(typeof(ParameterExpression));
                var param2 = Expression.Parameter(typeof(Expression));

                builder = Builders.GetOrAdd(
                    parameter.Type,
                    Expression
                        .Lambda<Func<ParameterExpression, Expression, object>>(
                            ReflectionUtils.Convert(
                                Expression.New(constructor, param1, param2),
                                typeof(object)),
                            param1, param2)
                        .Compile());
            }
            
            return builder(parameter, accessor);
        }
    }

    class QueryArgAccessor<TArgs> : IQueryArgAccessor<TArgs>
    {
        static readonly Func<TArgs, object> Identity = x => x;
        readonly Func<TArgs, object> GetParam;

        public QueryArgAccessor(ParameterExpression parameter, Expression accessor)
            : this(BuildGetter(parameter, accessor))
        {
        }

        public QueryArgAccessor(Func<TArgs, object> getParam)
        {
            GetParam = getParam;
        }

        public QueryArgAccessor()
            : this(Identity)
        {
        }

        object IQueryArgAccessor<TArgs>.GetArgValue(TArgs args) => GetParam(args);

        public static Func<TArgs, object> BuildGetter(ParameterExpression parameter, Expression accessor)
        {
            // compile accessor into function
            return Expression.Lambda<Func<TArgs, object>>(
                ReflectionUtils.Convert(accessor, typeof(object)), 
                parameter).Compile();
        }
    }
}
