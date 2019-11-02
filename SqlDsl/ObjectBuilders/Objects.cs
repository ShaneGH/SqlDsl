using SqlDsl.DataParser;
using SqlDsl.Mapper;
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
        public static Func<ObjectGraph, IPropMapValueCache, ILogger, T> CompileObjectBuilder<T>()
        {
            var type = typeof(T);
                
            // compile setter for each property
            var propSetters = type
                .GetFieldsAndProperties()
                .Where(p => !p.readOnly)
                .Select(p => (p.name, PropertySetters.GetPropertySetter<T>(p.type, p.name), p.type))
                .ToDictionary(x => x.Item1, x => (setter: x.Item2, type: x.Item3));

            // compile getters for each constructor type
            var cArgGetters = type
                .GetConstructors()
                .Select(c => c.GetParameters().Select(p => p.ParameterType).ToArray())
                .ToDictionary(x => x, x => x.Select(BuildValueGetter).ToArray(), ArrayComparer<Type>.Instance);

            T build(ObjectGraph vals, IPropMapValueCache propMapValueCache, ILogger logger) => BuildObjectWithExceptionHandling(cArgGetters, propSetters, vals, propMapValueCache, logger);
            return build;
        }

        static IValueGetter BuildValueGetter(Type forType)
        {
            return (IValueGetter)ReflectionUtils
                .GetMethod(() => BuildValueGetter<object>(), forType)
                .Invoke(null, EmptyObjects);
        }

        static ValueGetter<T> BuildValueGetter<T>()
        {
            var enumCount = ReflectionUtils.CountEnumerables(typeof(T));
            if (enumCount > 0)
            {
                var singleGetter = ValueGetters.GetValueGetter<T>(true, false);
                var enumerableGetter = ValueGetters.GetValueGetter<T>(enumCount > 1, true);

                return new ValueGetter<T>(singleGetter, enumerableGetter);
            }
            else
            {
                var getter = ValueGetters.GetValueGetter<T>(false, false);
                return new ValueGetter<T>(getter);
            }
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

        static readonly Type[] EmptyTypes = new Type[0];
        static readonly object[] EmptyObjects = new object[0];

        static T InvokeObjectConstructor<T>(
            Dictionary<Type[], IValueGetter[]> cArgGetters,
            ObjectGraph vals, 
            IPropMapValueCache propMapValueCache, 
            ILogger logger)
        {
            var constructorArgTypes = vals.ConstructorArgTypes ?? EmptyTypes;
            if (!cArgGetters.ContainsKey(constructorArgTypes))
                throw new InvalidOperationException($"Unable to find constructor for object {typeof(T)} with constructor args [{constructorArgTypes.JoinString(", ")}].");

            var constructor = cArgGetters[constructorArgTypes];
            var cargs = vals.GetSimpleConstructorArgs()
                .Select(SimpleConstructorArg)
                .Concat(vals.GetComplexConstructorArgs()
                    .OrEmpty()
                    .Select(BuildAndDisposeofComplexConstructorArg))
                .OrderBy(ca => ca.i)
                .Select((ca, i) => 
                {
                    if (i != ca.i)
                        throw new InvalidOperationException($"Expecting constructor arg {i}, but got {ca.i}.");

                    return ca.v;
                });

            // Create output object
            return (T)ConstructObject(
                typeof(T), 
                constructorArgTypes, 
                cargs.ToArray());

            (int i, object v) SimpleConstructorArg((int argIndex, IEnumerable<object> value, bool isEnumerableDataCell) arg) =>
                GetSimpleConstructorArg(constructor, arg, logger);

            (int i, object v) BuildAndDisposeofComplexConstructorArg((int argIndex, IEnumerable<ObjectGraph> value) arg) =>
                GetComplexConstructorArgAndDisposeObjectGraphs(constructorArgTypes, constructor, arg, propMapValueCache, logger);
        }

        /// <summary>
        /// Calls "BuildObject" with exception handling
        /// </summary>
        static T BuildObjectWithExceptionHandling<T>(
            Dictionary<Type[], IValueGetter[]> cArgGetters, 
            Dictionary<string, (PropertySetter<T> setter, Type propertyType)> propSetters, 
            ObjectGraph vals, 
            IPropMapValueCache propMapValueCache,
            ILogger logger)
        {
            // try
            // {
                return BuildObject(cArgGetters, propSetters, vals, propMapValueCache, logger);
            // }
            // catch (Exception e)
            // {
            //     throw new ParsingException($"Error creating object: {typeof(T)}.", e);
            // }
        }

        /// <summary>
        /// Build an object
        /// </summary>
        /// <param name="propSetters">A set of objects which can set the value of a property</param>
        /// <param name="vals">The values of the object</param>
        static T BuildObject<T>(
            Dictionary<Type[], IValueGetter[]> cArgGetters, 
            Dictionary<string, (PropertySetter<T> setter, Type propertyType)> propSetters, 
            ObjectGraph vals, 
            IPropMapValueCache propMapValueCache,
            ILogger logger)
        {
            if (vals == null)
                return (T)ConstructObject(typeof(T), EmptyTypes, EmptyObjects);

            var obj = ReflectionUtils.IsPropMapValue(typeof(T)) != null
                ? (T)propMapValueCache.ReleaseOrCreateItem()
                : InvokeObjectConstructor<T>(cArgGetters, vals, propMapValueCache, logger);

            // use a setter to set each simple property
            foreach (var prop in vals.GetSimpleProps().OrEmpty())
            {
                if (!propSetters.TryGetValue(prop.name, out (PropertySetter<T> setter, Type propertyType) setter))
                    continue;

                try
                {
                    switch (prop.isEnumerableDataCell)
                    {
                        case true:
                            setter.setter.SetEnumerable(obj, prop.value, logger);
                            break;
                        case false:
                            setter.setter.Set(obj, prop.value, logger);
                            break;
                    }
                }
                catch (Exception e)
                {
                    throw new ParsingException($"Error setting parameter: \"{prop.name}\".", e);
                }
            }

            foreach (var prop in vals.GetComplexProps())
            {
                if (!propSetters.TryGetValue(prop.name, out (PropertySetter<T> setter, Type propertyType) setter))
                {
                    foreach (var v in prop.value)
                        v.Dispose();
                        
                    continue;
                }
                    
                // test if the property is a T or IEnumerable<T>
                var singlePropertyType = 
                    ReflectionUtils.GetIEnumerableType(setter.propertyType) ??
                    setter.propertyType;

                // recurse to get actual values
                var builder = Builders.GetBuilder(singlePropertyType);
                var values = prop.value.Select(BuildAndDisposeofComplexProp);

                // set the value of the property
                setter.setter.Set(obj, values, logger);

                object BuildAndDisposeofComplexProp(ObjectGraph v)
                {
                    // TODO: there is a cast here (possibly a boxing if complex prop is struct)
                    var result = builder.Build(v, propMapValueCache, logger);
                    v.Dispose();
                    return result;
                }
            }

            return obj;
        }

        static (int index, object value) GetSimpleConstructorArg(IValueGetter[] argGetters, (int argIndex, IEnumerable<object> value, bool isEnumerableDataCell) arg, ILogger logger)
        {
            var getter = argGetters[arg.argIndex];
            var value = arg.isEnumerableDataCell ?
                getter.GetEnumerable(arg.value, logger) :
                getter.Get(arg.value, logger);

            return (arg.argIndex, value);
        }

        static (int index, object value) GetComplexConstructorArgAndDisposeObjectGraphs(Type[] constructorArgTypes, IValueGetter[] argGetters, (int argIndex, IEnumerable<ObjectGraph> value) arg, IPropMapValueCache propMapValueCache, ILogger logger)
        {
            var getter = argGetters[arg.argIndex];

            // test if the property is a T or IEnumerable<T>
            var singlePropertyType = 
                ReflectionUtils.GetIEnumerableType(constructorArgTypes[arg.argIndex]) ??
                constructorArgTypes[arg.argIndex];

            // recurse to get actual values
            var builder = Builders.GetBuilder(singlePropertyType);
            var values = arg.value.Select(BuildAndDispose);
                
            var value = getter.Get(values, logger);
            return (arg.argIndex, value);

            object BuildAndDispose(ObjectGraph v)
            {
                // TODO: there is a cast here (possibly a boxing if complex prop is struct)
                var result = builder.Build(v, propMapValueCache, logger);
                v.Dispose();
                return result;
            }
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
            var constr = type.GetConstructor(constructorArgTypes) ?? 
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
    }
}
