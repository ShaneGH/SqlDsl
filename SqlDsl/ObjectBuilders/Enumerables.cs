using SqlDsl.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// Utils to compile functions which deal with collections
    /// </summary>
    public static class Enumerables
    {
        /// <summary>
        /// Compile a function to set all null enumerable properties of an object to empty
        /// </summary>
        public static Action<T, IEnumerable<string>> CompileEnumerabeAdder<T>()
        {
            // get all members of the object
            var members = typeof(T).GetFieldsAndProperties();

            // for each member, attempt to build a collection creator function
            var enumerableProperties = members
                .Select(prop => (prop: prop, collection: CreateCollectionExpression(prop.Item2)))
                // filter out members where the create has failed
                .Where(x => x.Item2.isCollection);

            return CompileEnumerabeAdder<T>(enumerableProperties);
        }

        /// <summary>
        /// Compile a function to set all null enumerable properties of an object to empty
        /// </summary>
        static Action<T, IEnumerable<string>> CompileEnumerabeAdder<T>(IEnumerable<((string name, Type, bool isReadOnly) prop, (bool isCollection, Expression builder) collection)> enumerableProperties)
        {
            // the input of the output Action
            var input = Expression.Parameter(typeof(T));
            var enumerableDbFields = Expression.Parameter(typeof(IEnumerable<string>));
            var _null = Expression.Constant(null);

            // enumerableDbFields = enumerableDbFields.Enumerate();
            var enumerateInput = Expression.Assign(
                enumerableDbFields,
                Expression.Call(
                    ReflectionUtils.GetMethod<IEnumerable<string>>(x => x.Enumerate()),
                    enumerableDbFields));

            // for each property, build a setter
            var setters = enumerableProperties
                .Where(x => !x.prop.isReadOnly)
                // if (input.Prop == null && !enumerableDbFields.Contains("Prop")) { input.Prop = new List<...>(); }
                .Select(p => (Expression)Expression.IfThen(
                    Expression.And(
                        Expression.Equal(
                            Expression.PropertyOrField(input, p.prop.name),
                            _null),
                        Expression.Not(
                            Expression.Call(
                                ReflectionUtils.GetMethod<IEnumerable<string>>(
                                    x => x.Contains("")),
                                enumerableDbFields,
                                Expression.Constant(p.prop.name)))),
                    Expression.Assign(
                        Expression.PropertyOrField(input, p.prop.name),
                        p.collection.builder)));

            // compile all setters into a block
            var body = Expression.Block(setters.Prepend(enumerateInput));
            return Expression
                .Lambda<Action<T, IEnumerable<string>>>(body, input, enumerableDbFields)
                .Compile();
        }        

        /// <summary>
        /// Create an expression which builds a new collection.
        /// </summary>
        /// <param name="collectionType">can be T[], ICollection&lt;T>, IList&lt;T>, List&lt;T>, IEnumerable&lt;T>, IReadOnlyCollection&lt;T> or IReadOnlyList&lt;T>
        /// </param>
        /// <param name="collectionValues">If not null, the output builder will initialize the collection with these values
        /// </param>
        /// <param name="reuseCollectionIfPossible">If true, and the input collection is the right type, return it</param>
        /// <returns>(isCollection: the collectionType is valid, Expression: an expression to build the collection. null if isCollection == false)
        /// </returns>
        public static (bool isCollection, Expression builder) CreateCollectionExpression(Type collectionType, Expression collectionValues = null, bool reuseCollectionIfPossible = false)
        {
            var (ok, valueCreator) = CreateCollectionExpressionWithoutReuse(collectionType, collectionValues);
            if (!ok || !reuseCollectionIfPossible || collectionValues == null)
                return (ok, valueCreator);

            return (
                ok,
                Expression.IfThenElse(
                    Expression.TypeIs(collectionValues, collectionType),
                    collectionValues,
                    valueCreator));
        }
        
        private static (bool isCollection, Expression builder) CreateCollectionExpressionWithoutReuse(Type collectionType, Expression collectionValues)
        {
            if (collectionType.IsConstructedGenericType)
            {
                // convert List<T> into List<>
                var args = collectionType.GetGenericArguments();
                var genericDefinition = collectionType.GetGenericTypeDefinition();

                // build the correct collection for the given type
                if (args.Length == 1)
                {
                    if (genericDefinition == typeof(ICollection<>) ||
                        genericDefinition == typeof(IList<>) || 
                        genericDefinition == typeof(List<>))
                    {
                        return (true, BuildList(args[0], collectionValues));
                    }

                    if (genericDefinition == typeof(IEnumerable<>))
                    {
                        return (true, BuildIEnumerable(args[0], collectionValues));
                    }
                    
                    if (genericDefinition == typeof(IReadOnlyCollection<>) || 
                        genericDefinition == typeof(IReadOnlyList<>))
                    {
                        return (true, BuildReadOnlyCollection(args[0], collectionValues));
                    }
                    
                    if (genericDefinition == typeof(HashSet<>))
                    {
                        return (true, BuildHashSet(args[0], collectionValues));
                    }
                }
            }
            else if (collectionType.IsArray)
            {
                // build a valid Array
                return (true, BuildArray(ReflectionUtils.GetIEnumerableType(collectionType), collectionValues));
            }

            // collection cannot be built
            return (false, null);
        }

        /// <summary>
        /// Create an expression which builds a new List<T>.
        /// </summary>
        /// <param name="listOfType">The T in the List
        /// </param>
        /// <param name="valuesAsIEnumerable">If not null, the output builder will initialize the list with these values
        /// </param>
        /// <returns>an expression to build the list
        /// </returns>
        static Expression BuildList(Type listOfType, Expression valuesAsIEnumerable = null)
        {
            // get List<T> type
            var listType = typeof(List<>).MakeGenericType(new[] { listOfType });

            // if values is already a list, no need to build another
            if (valuesAsIEnumerable != null && listType.IsAssignableFrom(valuesAsIEnumerable.Type))
                return valuesAsIEnumerable;

            // get constructor for new List<T>(IEnumberable<T> v)
            var constructor = listType
                .GetConstructor(new Type[] { typeof(IEnumerable<>).MakeGenericType(listOfType) });

            // create expression, using Enumerable.Empty as default for values
            return Expression.New(constructor, valuesAsIEnumerable ?? BuildEmptyIEnumerable(listOfType));
        }

        /// <summary>
        /// Create an expression which builds a new HashSet<T>.
        /// </summary>
        /// <param name="setOfType">The T in the HashSet
        /// </param>
        /// <param name="valuesAsIEnumerable">If not null, the output builder will initialize the hash set with these values
        /// </param>
        /// <returns>an expression to build the hash set
        /// </returns>
        static Expression BuildHashSet(Type setOfType, Expression valuesAsIEnumerable = null)
        {
            // get HashSet<T> type
            var setType = typeof(HashSet<>).MakeGenericType(new[] { setOfType });

            // if values is already a set, no need to build another
            if (valuesAsIEnumerable != null && setType.IsAssignableFrom(valuesAsIEnumerable.Type))
                return valuesAsIEnumerable;

            // get constructor for new HashSet<T>(IEnumberable<T> v)
            var constructor = setType
                .GetConstructor(new Type[] { typeof(IEnumerable<>).MakeGenericType(setOfType) });

            // create expression, using Enumerable.Empty as default for values
            return Expression.New(constructor, valuesAsIEnumerable ?? BuildEmptyIEnumerable(setOfType));
        }

        /// <summary>
        /// Create an expression which builds a new IEnumerable<T>.
        /// </summary>
        /// <param name="enumerableOfType">The T in the enumerable
        /// </param>
        /// <param name="valuesAsIEnumerable">If not null, the output builder will initialize the IEnumerable with these values
        /// </param>
        /// <returns>an expression to build the IEnumerable
        /// </returns>
        static Expression BuildIEnumerable(Type enumerableOfType, Expression valuesAsIEnumerable) =>
            valuesAsIEnumerable ?? BuildEmptyIEnumerable(enumerableOfType);

        /// <summary>
        /// A cache of Enumerabe.Empty<T>() results
        /// </summary>
        static ConcurrentDictionary<Type, ConstantExpression> EmptyIEnumerables = new ConcurrentDictionary<Type, ConstantExpression>();
        
        /// <summary>
        /// Create an expression which builds an empty IEnumerable
        /// </summary>
        /// <param name="enumerableOfType">The T in the List
        /// </param>
        /// <returns>an expression to build the IEnumerable
        /// </returns>
        static Expression BuildEmptyIEnumerable(Type enumerableOfType)
        {
            // try to get value from cache
            if (EmptyIEnumerables.TryGetValue(enumerableOfType, out ConstantExpression val))
                return val;

            // use reflection to get value for Enumerable.Empty<enumerableOfType>()
            var empty = ReflectionUtils
                .GetMethod(() => Enumerable.Empty<object>(), enumerableOfType)
                .Invoke(null, new object[0]);

            // cache and return value as a constant expression
            return EmptyIEnumerables.GetOrAdd(enumerableOfType, Expression.Constant(empty));
        }

        /// <summary>
        /// Create an expression which builds a new ReadOnlyCollection<T>.
        /// </summary>
        /// <param name="collectionOfType">The T in the ReadOnlyCollection
        /// </param>
        /// <param name="values">If not null, the output builder will initialize the collection with these values
        /// </param>
        /// <returns>an expression to build the collection
        /// </returns>
        static Expression BuildReadOnlyCollection(Type collectionOfType, Expression values = null)
        {
            // get constructor: new ReadOnlyCollection<collectionOfType>(List<collectionOfType> vals);
            var constructor = typeof(ReadOnlyCollection<>)
                .MakeGenericType(new[] { collectionOfType })
                .GetConstructor(new Type[]
                {
                    typeof(List<>).MakeGenericType(new[] { collectionOfType })
                });

            // create list from values
            var list = BuildList(collectionOfType, values);

            // create expression from constructor and list
            return Expression.New(constructor, BuildList(collectionOfType, values));
        }

        /// <summary>
        /// A cache of new T[0] results
        /// </summary>
        static ConcurrentDictionary<Type, ConstantExpression> EmptyArrays = new ConcurrentDictionary<Type, ConstantExpression>();

        /// <summary>
        /// Create an expression which builds a new T[].
        /// </summary>
        /// <param name="arrayOfType">The T in the Array
        /// </param>
        /// <param name="values">If not null, the output builder will initialize the array with these values
        /// </param>
        /// <returns>an expression to build the array
        /// </returns>
        static Expression BuildArray(Type arrayOfType, Expression values = null)
        {
            if (values == null)
            {
                // if no values, try to get empty array from cache
                if (EmptyArrays.TryGetValue(arrayOfType, out ConstantExpression val))
                    return val;

                // create empty array
                var empty = Expression.Constant(
                    Array.CreateInstance(arrayOfType, 0));

                // add to cache and return
                return EmptyArrays.GetOrAdd(arrayOfType, empty);
            }

            // get method iEnumerableValues.ToArray<T>()
            var toArray = ReflectionUtils.GetMethod<IEnumerable<object>>(
                o => o.ToArray(),
                arrayOfType);

            // call toArray on input values
            return Expression.Call(null, toArray, values);
        }
    }
}
