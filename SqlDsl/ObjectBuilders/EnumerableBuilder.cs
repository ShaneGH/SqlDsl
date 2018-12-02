using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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

            CollectionBuilder = Expression
                .Lambda<Func<IEnumerable<T>, TCollection>>(createExpression, values)
                .Compile();
        }

        public TCollection Build(ReusableObjectGraph values, ILogger logger)
        {
            var objects = SplitObjectGraph(values, logger).Select(BuildSingleObject);
            return CollectionBuilder(objects);

            T BuildSingleObject(ReusableObjectGraph obj)
            {
                var result = SingleObjBuilder.Build(obj, logger);
                obj.Dispose();
                return result;
            }
        }

        object IBuilder.Build(ReusableObjectGraph values, ILogger logger) => Build(values, logger);

        /// <summary>
        /// Split an object graph in the form of {P1: [1, 2], P2: [3, 4]} into [{P1: [1], P2: [3]}, {P1: [2], P2: [4]}]
        /// </summary>
        static IEnumerable<ReusableObjectGraph> SplitObjectGraph(ReusableObjectGraph values, ILogger logger)
        {
            if (values.PropertyGraph.SimpleProps.Length == 0)
                yield break;

            // run a "Distinct" on the rowNumbers
            var dataRowsForProp = values.Objects
                .GroupBy(d => values.PropertyGraph.GetUniqueIdForSimpleProp(d, values.PropertyGraph.SimpleProps[0].rowNumberColumnIds))
                .Select(Enumerable.First);

            foreach (var row in dataRowsForProp)
            {
                var obj = values.Cache.GetGraph(logger);
                obj.Init(values.PropertyGraph, new[] { row });
                yield return obj;
            }
        }
    }
}
