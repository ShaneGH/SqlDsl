using SqlDsl.Utils;
using SqlDsl.Utils.EqualityComparers;
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
        static T BuildObject<T>(Dictionary<string, (Action<T, IEnumerable<object>, ILogger> setter, Type propertyType)> propSetters, ObjectGraph vals, ILogger logger)
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
                    propSetters[prop.name].setter(obj, prop.value, logger);
            }

            // use a setter to set each complex property
            foreach (var prop in vals.ComplexProps.OrEmpty())
            {
                if (!propSetters.ContainsKey(prop.name))
                    continue;
                    
                // test if the property is a T or IEnumerable<T>
                var singlePropertyType = 
                    ReflectionUtils.GetIEnumerableType(propSetters[prop.name].propertyType) ??
                    propSetters[prop.name].propertyType;

                // recurse to get actual values
                var builder = Builders.GetBuilder(singlePropertyType);
                var values = prop.value
                    // TODO: there is a cast here (possibly a boxing if complex prop is struct)
                    .Select(v => builder.Build(v, logger))
                    .Enumerate();

                // set the value of the property
                propSetters[prop.name].setter(obj, values, logger);
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
            new ConcurrentDictionary<Tuple<Type, Type[]>, Func<object[], object>>(ConstructorKeyComparer.Instance);

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
                    ReflectionUtils.Convert(
                        Expression.ArrayIndex(
                            args, 
                            Expression.Constant(i)),
                        x));

            return Ex
                .Lambda<Func<object[], object>>(
                    ReflectionUtils.Convert(
                        Ex.New(constr, constructorArgs),
                        typeof(object)),
                    args)
                .Compile();
        }

        /// <summary>
        /// Compile a function which sets the value of a property
        /// </summary>
        static Action<T, IEnumerable<object>, ILogger> BuildPropertySetter<T>(string propertyName, Type propertyType)
        {
            return (Action<T, IEnumerable<object>, ILogger>)ReflectionUtils
                .GetMethod(
                    () => BuildPropertySetter<object, object>(""),
                    typeof(T),
                    propertyType)
                    // todo: boxing cellTypeIsEnumerable
                .Invoke(null, new object[] { propertyName });
        }

        /// <summary>
        /// Compile a function which sets the value of a property
        /// </summary>
        static Action<T, IEnumerable<object>, ILogger> BuildPropertySetter<T, TProp>(string propertyName)
        {
            var builder = TypeConvertors.GetConvertor<TProp>(false);

            var setterObj = Ex.Parameter(typeof(T));
            var setterProp = Ex.Parameter(typeof(TProp));
            var setter = Ex
                .Lambda<Action<T, TProp>>(
                    Ex.Assign(
                        Ex.PropertyOrField(
                            setterObj,
                            propertyName),
                        setterProp),
                    setterObj,
                    setterProp)
                .Compile();

            return ReflectionUtils.GetIEnumerableType(typeof(TProp)) == null ?
                (Action<T, IEnumerable<object>, ILogger>)Set :
                SetEnumerable;

            void Set(T obj, IEnumerable<object> vals, ILogger logger)
            {
                var one = GetOne(propertyName, vals);
                setter(obj, builder(one, logger));
            }

            void SetEnumerable(T obj, IEnumerable<object> vals, ILogger logger)
            {
                setter(obj, builder(vals, logger));
            }
        }

        static readonly ConcurrentDictionary<EnumerableSettersKey, Action<object, IEnumerable, ILogger>> EnumerableSetterCache = new ConcurrentDictionary<EnumerableSettersKey, Action<object, IEnumerable, ILogger>>();

        public static Action<object, IEnumerable, ILogger> GetEnumerableSetter(Type objectType, string propertyName, Type resultPropertyType)
        {
            var key = new EnumerableSettersKey(objectType, propertyName, resultPropertyType);
            if (EnumerableSetterCache.TryGetValue(key, out Action<object, IEnumerable, ILogger> val))
                return val;

            return EnumerableSetterCache.GetOrAdd(key, BuildEnumerableSetter(objectType, propertyName, resultPropertyType));
        }

        static Action<object, IEnumerable, ILogger> BuildEnumerableSetter(Type objectType, string propertyName, Type resultPropertyType)
        {
            var obj = Ex.Parameter(typeof(object));
            var vals = Ex.Parameter(typeof(IEnumerable));
            var logger = Ex.Parameter(typeof(ILogger));
         
            // IEnumerable, ILogger -> collection type   
            var getter = TypeConvertors.BuildEnumerableConvertor(resultPropertyType, ReflectionUtils.GetIEnumerableType(resultPropertyType), true);
            var setter = Ex
                .Lambda<Action<object, IEnumerable, ILogger>>(
                    Ex.Assign(
                        Ex.PropertyOrField(
                            ReflectionUtils.Convert(obj, objectType), 
                            propertyName),
                        Ex.Invoke(
                            Ex.Constant(getter),
                            vals,
                            logger)),
                    obj,
                    vals,
                    logger)
                .Compile();

            return ReflectionUtils.CountEnumerables(resultPropertyType) <= 1 ?
                Single :
                setter;

            void Single(object o, IEnumerable v, ILogger l)
            {
                var val = GetOne(propertyName, v);
                setter(o, val as IEnumerable, l);
            }
        }

        /// <summary>
        /// If the enumerable contains 0 items, return default.
        /// If the enumerable contains 1 item, return it.
        /// If the enumerable contains more than 1 item, throw an exception
        /// </summary>
        public static object GetOne(string propertyName, IEnumerable items)
        {
            var enumerator = items.GetEnumerator();
            if (!enumerator.MoveNext())
                return null;

            var result = enumerator.Current;
            if (enumerator.MoveNext())
            {
                throw new InvalidOperationException($"Database has returned more than one item for " +
                    $"{propertyName}, however it only accepts a single item.");   
            }

            return result;
        }
    }
}
