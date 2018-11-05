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
    /// Helper to compile functions which create objects
    /// </summary>
    public static class ObjectConstructors
    {

        /// <summary>
        /// Build a constructor
        /// </summary>
        static Func<object[], T> BuildConstructor<T>(Type[] constructorArgTypes)
        {
            var type = typeof(T);

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
                .Lambda<Func<object[], T>>(
                    Ex.New(constr, constructorArgs),
                    args)
                .Compile();
        }
        // /// <summary>
        // /// Compile a function to build an object from an object graph
        // /// </summary>
        // public static Func<ObjectGraph, ILogger, T> CompileObjectBuilder<T>()
        // {
        //     var type = typeof(T);

        //     // different process for creating anonymous objects
        //     if (IsAnonymousType(type))
        //         return AnonymousObjects.CompileAnonymousObjectBuilder<T>();
                
        //     // compile setter for each property
        //     var propSetters = type
        //         .GetFieldsAndProperties()
        //         .Where(p => !p.readOnly)
        //         .Select(p => (name: p.name, setter: PropertySetters.GetPropertySetter<T>(p.type, p.name), p.type, ReflectionUtils.CountEnumerables(p.type)))
        //         //TODO: can enumerableDepth be part of the setter logic?
        //         .ToDictionary(x => x.name, x => (setter: x.Item2, type: x.Item3, enumerableDepth: x.Item4));

        //     T build(ObjectGraph vals, ILogger logger) => BuildObject(propSetters, vals, logger);
        //     return build;
        // }

        // /// <summary>
        // /// https://stackoverflow.com/questions/2483023/how-to-test-if-a-type-is-anonymous
        // /// </summary>
        // static bool IsAnonymousType(Type type)
        // {
        //     if (type == null)
        //         throw new ArgumentNullException(nameof(type));

        //     // HACK: The only way to detect anonymous types right now.
        //     return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
        //         && type.IsGenericType && type.Name.Contains("AnonymousType")
        //         && (type.Name.StartsWith("<>") || type.Name.StartsWith("VB$"))
        //         && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
        // }

        // static readonly Type[] EmptyTypes = new Type[0];
        // static readonly object[] EmptyObjects = new object[0];

        // /// <summary>
        // /// Build an object
        // /// </summary>
        // /// <param name="propSetters">A set of objects which can set the value of a property</param>
        // /// <param name="vals">The values of the object</param>
        // static T BuildObject<T>(Dictionary<string, (PropertySetter<T> setter, Type propertyType, int enumerableDepth)> propSetters, ObjectGraph vals, ILogger logger)
        // {
        //     if (vals == null)
        //         return (T)ConstructObject(typeof(T), EmptyTypes, EmptyObjects);

        //     // Create output object
        //     var obj = (T)ConstructObject(
        //         typeof(T), 
        //         vals.ConstructorArgTypes, 
        //         new object[0]);

        //     // use a setter to set each simple property
        //     foreach (var prop in vals.SimpleProps.OrEmpty())
        //     {
        //         if (!propSetters.ContainsKey(prop.name))
        //             continue;

        //         var setter = propSetters[prop.name];
        //         switch (prop.isEnumerableDataCell)
        //         {
        //             case true:
        //                 setter.setter.SetEnumerable(obj, prop.value, logger);
        //                 break;
        //             case false:
        //                 setter.setter.Set(obj, prop.value, logger);
        //                 break;
        //         }
        //     }

        //     // use a setter to set each complex property
        //     foreach (var prop in vals.ComplexProps.OrEmpty())
        //     {
        //         if (!propSetters.ContainsKey(prop.name))
        //             continue;
                    
        //         var setter = propSetters[prop.name];
                    
        //         // test if the property is a T or IEnumerable<T>
        //         var singlePropertyType = 
        //             ReflectionUtils.GetIEnumerableType(setter.propertyType) ??
        //             setter.propertyType;

        //         // recurse to get actual values
        //         var builder = Builders.GetBuilder(singlePropertyType);
        //         var values = prop.value
        //             // TODO: there is a cast here (possibly a boxing if complex prop is struct)
        //             .Select(v => builder.Build(v, logger))
        //             .Enumerate();

        //         // set the value of the property
        //         setter.setter.Set(obj, values, logger);
        //     }

        //     return obj;
        // }

        // /// <summary>
        // /// Create an object by calling its default constructor
        // /// </summary>
        // static object ConstructObject(Type type, Type[] constructorArgTypes, object[] constructorArgs) => GetConstructorFromCache(type, constructorArgTypes)(constructorArgs);

        // /// <summary>
        // /// A cahce of object constructors
        // /// </summary>
        // static readonly ConcurrentDictionary<Tuple<Type, Type[]>, Func<object[], object>> Constructors = 
        //     new ConcurrentDictionary<Tuple<Type, Type[]>, Func<object[], object>>(ConstructorKeyComparer.Instance);

        // /// <summary>
        // /// Get a cached constructor or build and add a new one to the cache
        // /// </summary>
        // static Func<object[], object> GetConstructorFromCache(Type type, Type[] constructorArgTypes)
        // {
        //     // try get object from cache
        //     var key = new Tuple<Type, Type[]>(type, constructorArgTypes);
        //     if (Constructors.TryGetValue(key, out Func<object[], object> constructor))
        //         return constructor;

        //     // add to cache and construct object
        //     return Constructors.GetOrAdd(key, BuildConstructor(type, constructorArgTypes));
        // }
    }
}
