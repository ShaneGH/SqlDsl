using SqlDsl.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

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

            // different process for creating anonymous objects
            if (IsAnonymousType(type))
                return AnonymousObjects.CompileAnonymousObjectBuilder<T>();
            
            // get the default construcor
            Func<T> constructor = () => (T)ConstructObject(type);
                
            var objectVar = Expression.Variable(type);

            // var objectVar = constructor()
            var createObject = Expression.Assign(
                objectVar, 
                Expression.Invoke(
                    Expression.Constant(constructor)));

            // get list of properties
            var props = type.GetFieldsAndProperties();
                
            // compile setter for each property
            var propSetters = props
                .Select(p => (name: p.name, BuildPropertySetter<T>(p.name, p.type), p.type))
                .ToDictionary(x => x.name, x => (setter: x.Item2, type: x.Item3));

            T build(ObjectGraph vals) => BuildObject(propSetters, vals);
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
                TypeConvertors.GetConvertor(iEnumerableType ?? propertyType));
            var castMethod = ReflectionUtils.GetMethod<IEnumerable<object>>(
                x => x.Select(_ => _), 
                typeof(object), iEnumerableType ?? propertyType);
            var valuesOfType = Expression.Call(null, castMethod, valParam, convertor);

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

                builder = Expression.Invoke(Expression.Constant(singularGetter), valuesOfType);
            }

            // objectParam.propertyName = valuesOfType
            var body = Expression.Assign(
                Expression.PropertyOrField(objectParam, propertyName),
                builder);

            return Expression
                .Lambda<Action<T, IEnumerable<object>>>(body, objectParam, valParam)
                .Compile();
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
                input = input.Enumerate();
                if (input.Count() > 1)
                    throw new InvalidOperationException($"Database has returned {input.Count()} items for " +
                        $"{propertyName}, however it only accepts a single item.");

                return input.FirstOrDefault();
            }
        }
    }
}
