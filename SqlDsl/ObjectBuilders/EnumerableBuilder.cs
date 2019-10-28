using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.DataParser.DataRow;
using SqlDsl.Mapper;
using SqlDsl.Utils;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// A compiled (at runtime) builder class which builds a concrete enumerable object from an object graph
    /// Should only be used as the root builder. It uses a convention which is a little quirky compared to
    /// other builders. See SplitObjectGraph(...) method for details
    /// </summary>
    public class EnumerableBuilder<TCollection, T> : IBuilder, IBuilder<TCollection>
        where TCollection : IEnumerable<T>
    {
        readonly Builder<T> SingleObjBuilder;
        readonly Func<IEnumerable<T>, TCollection> CollectionBuilder;

        public EnumerableBuilder(Builder<T> singleObjBuilder)
        {
            SingleObjBuilder = singleObjBuilder ?? throw new ArgumentNullException(nameof(singleObjBuilder));

            var values = Expression.Parameter(typeof(IEnumerable<>).MakeGenericType(new [] { typeof(T) }));
            var (isCollection, createExpression) = Enumerables.CreateCollectionExpression(
                typeof(TCollection),
                values);

            // will not occur because of generic constraint
            if (!isCollection)
            {
                throw new InvalidOperationException($"Type {typeof(TCollection)} must be an IEnumerable<>");
            }

            CollectionBuilder = EnsureCollectionIsNotIEnumerable(Expression
                .Lambda<Func<IEnumerable<T>, TCollection>>(createExpression, values))
                .Compile();
        }

        /// <summary>
        /// Convert IEnumerable&lt;T> => T[]
        /// </summary>
        static Expression<Func<IEnumerable<T>, TCollection>> EnsureCollectionIsNotIEnumerable(Expression<Func<IEnumerable<T>, TCollection>> builder)
        {
            var enumerableType = ReflectionUtils.GetIEnumerableType(builder.Body.Type, recurse: false, strict: true);
            if (enumerableType == null)
                return builder;

            // xs => (IEnumerable<T>)xs.Enumerate()
            return Expression.Lambda<Func<IEnumerable<T>, TCollection>>(
                Expression.Convert(
                    Expression.Call(
                        ReflectionUtils.GetMethod<IEnumerable<object>>(xs => xs.Enumerate(), enumerableType),
                        builder.Body),
                    builder.Body.Type),
                builder.Parameters);
        }

        public TCollection Build(ObjectGraph values, IPropMapValueCache propMapValueCache, ILogger logger)
        {
            var objects = SplitObjectGraph(values, logger).Select(BuildSingleObject);
            return CollectionBuilder(objects);

            T BuildSingleObject(ObjectGraph obj)
            {
                var result = SingleObjBuilder.Build(obj, propMapValueCache, logger);
                obj.Dispose();
                return result;
            }
        }

        object IBuilder.Build(ObjectGraph values, IPropMapValueCache propMapValueCache, ILogger logger) => Build(values, propMapValueCache, logger);

        /// <summary>
        /// Split an object graph in the form of {P1: [1, 2], P2: [3, 4]} into [{P1: [1], P2: [3]}, {P1: [2], P2: [4]}]
        /// </summary>
        static IEnumerable<ObjectGraph> SplitObjectGraph(ObjectGraph values, ILogger logger)
        {
            if (values.PropertyGraph.SimpleConstructorArgs.Length == 0 && values.PropertyGraph.SimpleProps.Length == 0)
            {
                foreach (var obj in values.Objects)
                    yield return values.Clone();

                yield break;
            }

            var key = values.PropertyGraph.SimpleConstructorArgs.Length == 0
                ? values.PropertyGraph.SimpleProps[0].PrimaryKeyColumns
                : values.PropertyGraph.SimpleConstructorArgs[0].PrimaryKeyColumns;

            // run a "Distinct" on the rowNumbers
            var dataRowsForProp = values.PropertyGraph.GetDataRowsForSimpleProperty(values.Objects, key);

            foreach (var row in dataRowsForProp)
            {
                var obj = values.Cache.ReleseOrCreateItem();
                obj.Init(values.PropertyGraph, new[] { row });
                yield return obj;
            }
        }
    }
}
