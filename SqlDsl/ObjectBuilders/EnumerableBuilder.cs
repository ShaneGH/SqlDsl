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
    public class EnumerableBuilder<TCollection, T> : IBuilder
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

        /// <summary>
        /// Slit an object graph in the form of {P1: [1, 2], P2: [3, 4]} into [{P1: [1], P2: [3]}, {P1: [2], P2: [4]}]
        /// </summary>
        static IEnumerable<ObjectGraph> SplitObjectGraph(ObjectGraph values)
        {
            // this is a bit of a hack.
            // when this is used as the root builder, the parser
            // will amalgamate props of different objects into one

            // TODO: need to do some significant preformace testing on this method

            var enumerators = values.SimpleProps
                .Select(p => (prop: p, en: p.value.GetEnumerator()))
                .ToArray();

            if (!enumerators.Any())
                return Enumerable.Empty<ObjectGraph>();

            var vals = new List<ObjectGraph>();
            var exceptions = new List<Exception>();
            try
            {
                while (enumerators[0].en.MoveNext())
                {
                    var newProps = new List<(string name, IEnumerable<object> value, Action<object, IEnumerable, ILogger> customSetter, bool isEnumerableDataCell)>
                    {
                        (
                            enumerators[0].prop.name, 
                            new [] { enumerators[0].en.Current }, 
                            enumerators[0].prop.customSetter, 
                            enumerators[0].prop.isEnumerableDataCell)
                    };

                    foreach (var en in enumerators.Skip(1))
                    {
                        if (!en.en.MoveNext())
                        {
                            throw new InvalidOperationException($"Expected to have value for property {en.prop.name}");
                        }

                        newProps.Add((en.prop.name, new [] { en.en.Current }, en.prop.customSetter, en.prop.isEnumerableDataCell)); 
                    }

                    vals.Add(new ObjectGraph
                    {
                        SimpleProps = newProps
                    });
                }
                
                foreach (var en in enumerators.Skip(1))
                {
                    if (en.en.MoveNext())
                    {
                        throw new InvalidOperationException($"Expected not to have value for property {en.prop.name}");
                    }
                }
            }
            catch (Exception e)
            {
                exceptions.Add(e);
            }
            finally
            {
                foreach (var en in enumerators)
                {
                    try
                    {
                        en.en.Dispose();
                    }
                    catch (Exception e)
                    {
                        exceptions.Add(e);
                    }
                }
            }

            if (exceptions.Count == 1)
                throw exceptions[0];
            if (exceptions.Count > 1)
                throw new AggregateException("Exceptions occurred when building enumerable object and then when disposing of enumerators", exceptions);

            return vals;
        }

        public TCollection Build(ObjectGraph values, ILogger logger)
        {
            var objects = SplitObjectGraph(values).Select(x => SingleObjBuilder.Build(x, logger));
            return CollectionBuilder(objects);
        }

        object IBuilder.Build(ObjectGraph values, ILogger logger) => Build(values, logger);
    }
}
