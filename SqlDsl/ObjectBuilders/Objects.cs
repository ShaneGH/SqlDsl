using SqlDsl.Utils;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace SqlDsl.ObjectBuilders
{
    using Ex = Expression;

    /// <summary>
    /// Helper to compile functions which will convert an object graph into a concrete object
    /// </summary>
    public static class Objects
    {
        /// <summary>
        /// Compile a function to build an object from an object graph
        /// </summary>
        public static Func<ObjectGraph, ILogger, T> CompileObjectBuilder<T>()
        {
            var type = typeof(T);

            // different process for creating anonymous objects
            if (IsAnonymousType(type))
                return AnonymousObjects.CompileAnonymousObjectBuilder<T>();
                
            // compile setter for each property
            var propSetters = type
                .GetFieldsAndProperties()
                .Where(p => !p.readOnly)
                .Select(p => (name: p.name, BuildPropertySetter<T>(p.name, p.type), p.type))
                .ToDictionary(x => x.name, x => (setter: x.Item2, type: x.Item3));

            T build(ObjectGraph vals, ILogger logger) => BuildObject(propSetters, vals, logger);
            return build;
        }

        /// <summary>
        /// https://stackoverflow.com/questions/2483023/how-to-test-if-a-type-is-anonymous
        /// </summary>
        static bool IsAnonymousType(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            // HACK: The only way to detect anonymous types right now.
            return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
                && type.IsGenericType && type.Name.Contains("AnonymousType")
                && (type.Name.StartsWith("<>") || type.Name.StartsWith("VB$"))
                && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
        }

        public static readonly Type[] EmptyTypes = new Type[0];
        public static readonly object[] EmptyObjects = new object[0];

        /// <summary>
        /// Build an object
        /// </summary>
        /// <param name="propSetters">A set of objects which can set the value of a property</param>
        /// <param name="vals">The values of the object</param>
        static T BuildObject<T>(Dictionary<string, (Action<T, IEnumerable<object>> setter, Type propertyType)> propSetters, ObjectGraph vals, ILogger logger)
        {
            if (vals == null)
                return (T)ConstructObject(typeof(T), EmptyTypes, EmptyObjects);

            // Create output object
            var obj = (T)ConstructObject(
                typeof(T), 
                vals.ConstructorArgTypes, 
                new object[0]);

            // use a setter to set each simple property
            foreach (var prop in vals.SimpleProps.OrEmpty())
            {
                // if the prop has a custom setter, use it
                // otherwise use the default prop setters
                if (prop.customSetter != null)
                    prop.customSetter(obj, prop.value, logger);
                else if (propSetters.ContainsKey(prop.name))
                    propSetters[prop.name].setter(obj, prop.value);
            }

            // use a setter to set each complex property
            foreach (var prop in vals.ComplexProps.OrEmpty())
            {
                if (propSetters.ContainsKey(prop.name))
                {
                    // test if the property is a T or IEnumerable<T>
                    var singlePropertyType = 
                        ReflectionUtils.GetIEnumerableType(propSetters[prop.name].propertyType) ??
                        propSetters[prop.name].propertyType;

                    // recurse to get actual values
                    var values = prop.value
                        .Select(v => Builders.Build(singlePropertyType, v, logger))
                        .Enumerate();

                    // set the value of the property
                    propSetters[prop.name].setter(obj, values);
                }
            }

            return obj;
        }

        /// <summary>
        /// Create an object by calling its default constructor
        /// </summary>
        static object ConstructObject(Type type, Type[] constructorArgTypes, object[] constructorArgs) => GetConstructorFromCache(type, constructorArgTypes)(constructorArgs);

        /// <summary>
        /// A cahce of object constructors
        /// </summary>
        static readonly ConcurrentDictionary<Tuple<Type, Type[]>, Func<object[], object>> Constructors = 
            new ConcurrentDictionary<Tuple<Type, Type[]>, Func<object[], object>>(new ConstructorKeyComparer());

        /// <summary>
        /// Get a cached constructor or build and add a new one to the cache
        /// </summary>
        static Func<object[], object> GetConstructorFromCache(Type type, Type[] constructorArgTypes)
        {
            // try get object from cache
            var key = new Tuple<Type, Type[]>(type, constructorArgTypes);
            if (Constructors.TryGetValue(key, out Func<object[], object> constructor))
                return constructor;

            // add to cache and construct object
            return Constructors.GetOrAdd(key, BuildConstructor(type, constructorArgTypes));
        }

        /// <summary>
        /// Build a constructor
        /// </summary>
        static Func<object[], object> BuildConstructor(Type type, Type[] constructorArgTypes)
        {
            // get default constructor for object
            var constr = type
                .GetConstructor(constructorArgTypes) ?? 
                throw new InvalidOperationException(
                    $"Object {type} does not have a constructor with args: {constructorArgTypes.JoinString(", ")}");

            // compile expression for constructor
            var args = Expression.Parameter(typeof(object[]));
            var constructorArgs = constructorArgTypes
                .Select((x, i) => 
                    Expression.Convert(
                        Expression.ArrayIndex(
                            args, 
                            Expression.Constant(i)),
                        x));

            return Ex
                .Lambda<Func<object[], object>>(
                    Ex.Convert(
                        Ex.New(constr, constructorArgs),
                        typeof(object)),
                    args)
                .Compile();
        }

        /// <summary>
        /// Compile a function which sets the value of a property
        /// </summary>
        static Action<T, IEnumerable<object>> BuildPropertySetter<T>(string propertyName, Type propertyType)
        {
            // get the type of the property if it is inside an IEnumerable
            var iEnumerableType = ReflectionUtils.GetIEnumerableType(propertyType);
            
            // Build expression inputs
            var objectParam = Ex.Parameter(typeof(T));
            var valParam = Ex.Parameter(typeof(IEnumerable<object>));

            // vals.Select(x => Convertors.GetConvertor(propertyType)(x))
            var convertor = Ex.Constant(
                TypeConvertors.GetConvertor(iEnumerableType ?? propertyType));
            var castMethod = ReflectionUtils.GetMethod<IEnumerable<object>>(
                x => x.Select(_ => _), 
                typeof(object), iEnumerableType ?? propertyType);
            var valuesOfType = Ex.Call(null, castMethod, valParam, convertor);

            // if property is enumerable: new List<T>(valuesOfType)
            // otherwise null
            var (isCollection, builder) = Enumerables
                .CreateCollectionExpression(propertyType, valuesOfType);

            // if the property is not an enumerable
            if (!isCollection)
            {
                // create a function which will allow 
                // 0 or 1 values in an array of inputs
                // for a non enumerable result
                var singularGetter = ReflectionUtils
                    .GetMethod(() => BuildGetterForSingularProp<int>(""), propertyType)
                    .Invoke(null, new [] { propertyName });

                builder = Ex.Invoke(Ex.Constant(singularGetter), valuesOfType);
            }

            // objectParam.propertyName = valuesOfType
            var body = Ex.Assign(
                Ex.PropertyOrField(objectParam, propertyName),
                builder);

            return Ex
                .Lambda<Action<T, IEnumerable<object>>>(body, objectParam, valParam)
                .Compile();
        }

        /// <summary>
        /// If the enumerable contains 0 items, return default.
        /// If the enumerable contains 1 item, return it.
        /// If the enumerable contains more than 1 item, throw an exception
        /// </summary>
        public static T GetOne<T>(string propertyName, IEnumerable<T> items)
        {
            using (var enumerator = items.GetEnumerator())
            {
                if (!enumerator.MoveNext())
                    return default(T);

                var result = enumerator.Current;
                if (enumerator.MoveNext())
                {
                    throw new InvalidOperationException($"Database has returned more than one item for " +
                        $"{propertyName}, however it only accepts a single item.");   
                }

                return result;
            }
        }

        /// <summary>
        /// Create a function which converts an enumerable value to a singular one,
        /// throwing an exception if there is more than 1 value
        /// </summary>
        static Func<IEnumerable<T>, T> BuildGetterForSingularProp<T>(string propertyName)
        {
            return Get;

            T Get(IEnumerable<T> input)
            {
                return GetOne(propertyName, input);
            }
        }

        static readonly ConcurrentDictionary<EnumerableSettersKey, Action<object, IEnumerable<object>, ILogger>> EnumerableSetterCache = new ConcurrentDictionary<EnumerableSettersKey, Action<object, IEnumerable<object>, ILogger>>();

        public static Action<object, IEnumerable<object>, ILogger> GetEnumerableSetter(Type objectType, string propertyName, Type enumeratedType, Type resultPropertyType)
        {
            var key = new EnumerableSettersKey(objectType, propertyName, enumeratedType, resultPropertyType);
            if (EnumerableSetterCache.TryGetValue(key, out Action<object, IEnumerable<object>, ILogger> val))
                return val;

            return EnumerableSetterCache.GetOrAdd(key, BuildEnumerableSetter(objectType, propertyName, enumeratedType, resultPropertyType));
        }

        static Action<object, IEnumerable<object>, ILogger> BuildEnumerableSetter(Type objectType, string propertyName, Type enumeratedType, Type resultPropertyType)
        {
            //TODO: is enumeratedType arg needed?

            // ((objectType)objParam).propertyName = (resultPropertyType)valueParam
            var objParam = Ex.Parameter(typeof(object));
            var valueParam = Ex.Parameter(typeof(object));
            var setterBody = Ex.Assign(
                Ex.PropertyOrField(
                    Ex.Convert(
                        objParam,
                        objectType),
                    propertyName),
                Ex.Convert(
                    valueParam,
                    resultPropertyType));

            var setter = Ex
                .Lambda<Action<object, object>>(setterBody, objParam, valueParam)
                .Compile();

            var ensureCollectionType = EnsureCollectionType(resultPropertyType, propertyName);
            var singleCollectionOnly = ReflectionUtils.CountEnumerables(resultPropertyType) <= 1;

            return Setter;
            
            void Setter(object obj, IEnumerable<object> values, ILogger logger)
            {
                var value = singleCollectionOnly ?
                    GetOne(propertyName, values) :
                    values;

                if (value == DBNull.Value)
                    value = null;
                else if (value != null)
                    value = ensureCollectionType(value, logger);
                
                setter(obj, value);
            }
        }
        
        /// <summary>
        /// Given a (nested) collection type, create a function which takes in 
        /// an object with variable collection types and converts them to the correct
        /// type.
        /// e.g. collectionType = List&lt;byte[]>, obj = (object)IEnumerable&lt;IEnumerable&lt;byte>>
        /// Returns null if collectionType is not actually a collection
        /// </summary>
        static Func<object, ILogger, object> EnsureCollectionType(Type collectionType, string propertyName)
        {
            var collectionTypeEnumerable = ReflectionUtils.GetIEnumerableType(collectionType);
            if (collectionTypeEnumerable == null)
            {
                return null;
            }

            var iEnumerableOfType = typeof(IEnumerable<>).MakeGenericType(collectionTypeEnumerable);
            var input = Ex.Parameter(typeof(object));
            var loggerArg = Ex.Parameter(typeof(ILogger));

            Expression inputAsEnumerable;
            
            // if enumType is also enum => IEnumerable<IEnumerable<T>> = IEnumerable<object>.Select(EnsureCollectionType)
            var innerCollectionBuilder = EnsureCollectionType(collectionTypeEnumerable, propertyName);
            if (innerCollectionBuilder != null)
            {
                var casterInput = Ex.Parameter(typeof(object));

                // cast output of innerCollectionBuilder to byte array
                var castedCollectionBuilder = Ex
                    .Lambda(
                        Ex.Convert(
                            Ex.Invoke(
                                Ex.Constant(innerCollectionBuilder),
                                casterInput,
                                Ex.Constant(null, typeof(ILogger))),
                            collectionTypeEnumerable),
                        casterInput)
                    .Compile();

                inputAsEnumerable = Ex.Call(
                    null,
                    ReflectionUtils.GetMethod<IEnumerable<object>>(
                        xs => xs.Select(x => x),
                        typeof(object),
                        collectionTypeEnumerable),
                    Ex.Convert(
                        input,
                        typeof(IEnumerable<object>)),
                    Ex.Constant(castedCollectionBuilder));  
            }
            else
            {
                // cast object => IEnumerable<T>
                inputAsEnumerable = Ex.Convert(
                    input,
                    iEnumerableOfType);
            }

            var (isCollection, cr) = Enumerables.CreateCollectionExpression(
                collectionType, 
                inputAsEnumerable);

            if (!isCollection)
            {
                throw new InvalidOperationException(
                    $"Property {propertyName} ({collectionType}) must be a collection type " + 
                    "(e.g. List<T>, T[], IEnumerable<T>).");   
            }

            var create = Ex
                .Lambda<Func<object, ILogger, object>>(cr, input, loggerArg)
                .Compile();
            
            return Ensure;

            object Ensure(object values, ILogger logger)
            {
                if (collectionType.IsAssignableFrom(values.GetType()))
                {
                    return values;
                }

                if (logger.CanLogWarning())
                {
                    var valsType = GetTypeString(values);
                    logger.LogWarning($"Converting {valsType} to type {collectionType} for property " + 
                        $"\"{propertyName}\". This conversion is inefficient. Consider changing the " + 
                        $"data type of \"{propertyName}\" to {valsType}");
                }

                try
                {
                    return create(values, logger);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException(
                        $"Value {GetTypeString(values)} cannot be converted to type {collectionType}", e);
                }
            }
        }

        static string GetTypeString(object values)
        {
            if (values == null) return "?";

            var valType = values.GetType();
            if (!(values is IEnumerable))
                return valType.Name;

            var enumer = (values as IEnumerable).GetEnumerator();
            enumer.MoveNext();

            return valType.IsArray ?
                (GetTypeString(enumer.Current) + "[]") :
                (valType.Name + "<" + GetTypeString(enumer.Current) + ">");
        }

        class ConstructorKeyComparer : IEqualityComparer<Tuple<Type, Type[]>>
        {
            public bool Equals(Tuple<Type, Type[]> x, Tuple<Type, Type[]> y)
            {
                if (x == null && y == null) return true;
                if (x == null || y == null) return false;

                if (x.Item1 != y.Item1) return false;
                
                if (x.Item2 == null && y.Item2 == null) return true;
                if (x.Item2 == null || y.Item2 == null) return false;

                if (x.Item2.Length != y.Item2.Length) return false;

                for (var i = 0; i < x.Item2.Length; i++)
                {
                    if (x.Item2[i] != y.Item2[i]) return false;
                }

                return true;
            }

            public int GetHashCode(Tuple<Type, Type[]> obj)
            {
                return obj.Item2
                    .Aggregate(obj.Item1.GetHashCode(), (hash, t) => hash ^ t.GetHashCode());
            }
        }
    }
}
