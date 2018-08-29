using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// Helper to compile functions which will convert an object graph into a concrete object
    /// </summary>
    public static class Objects
    {
        /// <summary>
        /// Compile a function to build an object from an object graph
        /// </summary>
        public static Func<ObjectGraph, T> CompileObjectBuilder<T>()
        {
            var type = typeof(T);

            // get the default construcor
            var constructor = type
                .GetConstructors()
                .FirstOrDefault(c => !c.GetParameters().Any()) ??
                throw new InvalidOperationException($"Object {type} must have a paramaterless constructor");

            // expression input arg
            var objectVar = Expression.Variable(type);

            // use constructor
            var createObject = Expression.Assign(objectVar, Expression.New(constructor));

            // get list of properties
            var pi = BindingFlags.Public | BindingFlags.Instance;
            var props = type
                .GetProperties(pi)
                .Select(p => (p.Name, p.PropertyType))
                .Concat(type.GetFields(pi).Select(p => (p.Name, p.FieldType)));
                
            // compile setter for each property
            var propSetters = props
                .Select(p => (name: p.Name, BuildPropertySetter<T>(p.Name, p.Item2), p.Item2))
                .ToDictionary(x => x.name, x => (setter: x.Item2, type: x.Item3));

            T build(ObjectGraph vals) => BuildObject(propSetters, vals);
            return build;
        }

        /// <summary>
        /// Build an object
        /// </summary>
        /// <param name="propSetters">A set of objects which can set the value of a property</param>
        /// <param name="vals">The values of the object</param>
        static T BuildObject<T>(Dictionary<string, (Action<T, IEnumerable<object>> setter, Type propertyType)> propSetters, ObjectGraph vals)
        {
            // Create output object
            var obj = (T)ConstructObject(typeof(T));
            if (vals == null)
                return obj;

            // use a setter to set each simple property
            foreach (var prop in vals.SimpleProps.OrEmpty())
                if (propSetters.ContainsKey(prop.name))
                    // TODO: can prop.value be enumerable?
                    propSetters[prop.name].setter(obj, prop.value);
            
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
                        .Select(v => Builders.Build(singlePropertyType, v))
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
        static object ConstructObject(Type type) => GetConstructorFromCache(type)();

        /// <summary>
        /// A cahce of object constructors
        /// </summary>
        static readonly ConcurrentDictionary<Type, Func<object>> Constructors = new ConcurrentDictionary<Type, Func<object>>();

        /// <summary>
        /// Get a cached constructor or build and add a new one to the cache
        /// </summary>
        static Func<object> GetConstructorFromCache(Type type)
        {
            // try get object from cache
            if (Constructors.TryGetValue(type, out Func<object> constructor))
                return constructor;

            // get default constructor for object
            var constr = type
                .GetConstructors()
                .Where(c => c.GetParameters().Length == 0)
                .FirstOrDefault() ?? 
                throw new InvalidOperationException($"Object {type} does not have a default constructor");

            // compile expression for constructor
            constructor = Expression
                .Lambda<Func<object>>(
                    Expression.Convert(
                        Expression.New(constr),
                        typeof(object)))
                .Compile();

            // add to cache and construct object
            return Constructors.GetOrAdd(type, constructor);
        }

        /// <summary>
        /// Compile a function which sets the value of a property
        /// </summary>
        static Action<T, IEnumerable<object>> BuildPropertySetter<T>(string propertyName, Type propertyType)
        {
            // get the type of the property if it is inside an IEnumerable
            var iEnumerableType = ReflectionUtils.GetIEnumerableType(propertyType);
            
            // Build expression inputs
            var objectParam = Expression.Parameter(typeof(T));
            var valParam = Expression.Parameter(typeof(IEnumerable<object>));

            // vals.Select(x => Convertors.GetConvertor(propertyType)(x))
            var convertor = Expression.Constant(
                TypeConvertors.GetConvertor(iEnumerableType ?? propertyType, true));
            var castMethod = ReflectionUtils.GetMethod<IEnumerable<object>>(
                x => x.Select(_ => _), 
                typeof(object), iEnumerableType ?? propertyType);
            var valuesOfType = Expression.Call(null, castMethod, valParam, convertor);

            // if property is enumerable: new List<T>(valuesOfType)
            // otherwise null
            var getter = Enumerables
                .CreateCollectionExpression(propertyType, valuesOfType);

            // if the property is not an enumerable
            if (!getter.isCollection)
            {
                // use test function to check against too many values
                Action<IEnumerable<object>> testForList = input =>
                {
                    if (input.Count() > 1)
                        throw new InvalidOperationException($"Database has returned {input.Count()} items for " +
                            $"{propertyName}, however it only accepts a single item.");
                };

                var checkForTooMany = Expression.Invoke(
                    Expression.Constant(testForList),
                    valParam);

                var firstOrDefault = ReflectionUtils.GetMethod<IEnumerable<object>>(
                    x => x.FirstOrDefault(), 
                    propertyType);

                getter = (true, Expression.Block(
                    // testForList(values)
                    checkForTooMany,
                    // valuesOfType.FirstOrDefault()
                    Expression.Call(null, firstOrDefault, valuesOfType)
                ));
            }

            var body = Expression.Assign(
                Expression.PropertyOrField(objectParam, propertyName),
                getter.builder);

            return Expression.Lambda<Action<T, IEnumerable<object>>>(body, objectParam, valParam).Compile();
        }
    }
}
