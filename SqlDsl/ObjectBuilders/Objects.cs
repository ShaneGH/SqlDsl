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
                .Select(p => (p.name, PropertySetters.GetPropertySetter<T>(p.type, p.name), p.type))
                //TODO: can enumerableDepth be part of the setter logic?
                .ToDictionary(x => x.Item1, x => (setter: x.Item2, type: x.Item3));

            // compile getters for each constructor type
            var cArgGetters = type
                .GetConstructors()
                .Select(c => c.GetParameters().Select(p => p.ParameterType).ToArray())
                .ToDictionary(x => x, x => x.Select(BuildValueGetter).ToArray(), ArrayComparer<Type>.Instance);

            T build(ObjectGraph vals, ILogger logger) => BuildObject(cArgGetters, propSetters, vals, logger);
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

        /// <summary>
        /// Build an object
        /// </summary>
        /// <param name="propSetters">A set of objects which can set the value of a property</param>
        /// <param name="vals">The values of the object</param>
        static T BuildObject<T>(
            Dictionary<Type[], IValueGetter[]> cArgGetters, 
            Dictionary<string, (PropertySetter<T> setter, Type propertyType)> propSetters, 
            ObjectGraph vals, 
            ILogger logger)
        {
            if (vals == null)
                return (T)ConstructObject(typeof(T), EmptyTypes, EmptyObjects);

            var constructorArgTypes = vals.ConstructorArgTypes ?? EmptyTypes;
            if (!cArgGetters.ContainsKey(constructorArgTypes))
                throw new InvalidOperationException($"Unable to find constructor for object {typeof(T)} with constructor args [{constructorArgTypes.JoinString(", ")}].");

            var constructor = cArgGetters[constructorArgTypes];
            var cargs = vals.SimpleConstructorArgs
                .OrEmpty()
                .Select(SimpleConstructorArg)
                .Concat(vals.ComplexConstructorArgs
                    .OrEmpty()
                    .Select(ComplexConstructorArg))
                .OrderBy(ca => ca.i)
                .Select((ca, i) => 
                {
                    if (i != ca.i)
                        throw new InvalidOperationException($"Expecting constructor arg {i}, but got {ca.i}.");

                    return ca.v;
                });

            // Create output object
            var obj = (T)ConstructObject(
                typeof(T), 
                constructorArgTypes, 
                cargs.ToArray());

            // use a setter to set each simple property
            foreach (var prop in vals.SimpleProps.OrEmpty())
            {
                if (!propSetters.ContainsKey(prop.name))
                    continue;

                var setter = propSetters[prop.name];
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

            // use a setter to set each complex property
            foreach (var prop in vals.ComplexProps.OrEmpty())
            {
                if (!propSetters.ContainsKey(prop.name))
                    continue;
                    
                var setter = propSetters[prop.name];
                    
                // test if the property is a T or IEnumerable<T>
                var singlePropertyType = 
                    ReflectionUtils.GetIEnumerableType(setter.propertyType) ??
                    setter.propertyType;

                // recurse to get actual values
                var builder = Builders.GetBuilder(singlePropertyType);
                var values = prop.value
                    // TODO: there is a cast here (possibly a boxing if complex prop is struct)
                    .Select(v => builder.Build(v, logger))
                    .Enumerate();

                // set the value of the property
                setter.setter.Set(obj, values, logger);
            }

            return obj;

            (int i, object v) SimpleConstructorArg((int argIndex, IEnumerable<object> value, bool isEnumerableDataCell) arg) =>
                GetSimpleConstructorArg(constructor, arg, logger);

            (int i, object v) ComplexConstructorArg((int argIndex, IEnumerable<ObjectGraph> value) arg) =>
                GetComplexConstructorArg(constructorArgTypes, constructor, arg, logger);
        }

        static (int index, object value) GetSimpleConstructorArg(IValueGetter[] argGetters, (int argIndex, IEnumerable<object> value, bool isEnumerableDataCell) arg, ILogger logger)
        {
            var getter = argGetters[arg.argIndex];
            var value = arg.isEnumerableDataCell ?
                getter.GetEnumerable(arg.value, logger) :
                getter.Get(arg.value, logger);

            return (arg.argIndex, value);
        }

        static (int index, object value) GetComplexConstructorArg(Type[] constructorArgTypes, IValueGetter[] argGetters, (int argIndex, IEnumerable<ObjectGraph> value) arg, ILogger logger)
        {
            var getter = argGetters[arg.argIndex];

            // test if the property is a T or IEnumerable<T>
            var singlePropertyType = 
                ReflectionUtils.GetIEnumerableType(constructorArgTypes[arg.argIndex]) ??
                constructorArgTypes[arg.argIndex];

            // recurse to get actual values
            var builder = Builders.GetBuilder(singlePropertyType);
            var values = arg.value
                // TODO: there is a cast here (possibly a boxing if complex prop is struct)
                .Select(v => builder.Build(v, logger))
                .Enumerate();
                
            var value = getter.Get(values, logger);
            return (arg.argIndex, value);
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
