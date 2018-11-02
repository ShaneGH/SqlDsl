using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using SqlDsl.Utils;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// A cache of compiled builders to create concrete classes
    /// </summary>
    public static class Builders
    {
        /// <summary>
        /// The cache of builders per type
        /// </summary>
        static readonly ConcurrentDictionary<Type, IBuilder> _Builders = new ConcurrentDictionary<Type, IBuilder>();

        /// <summary>
        /// Build an object of a given type with the object graph provided
        /// </summary>
        public static object Build(Type type, ObjectGraph values, ILogger logger)
        {
            // use cached builder to create concrete object
            return GetBuilder(type).Build(values, logger);
        }

        /// <summary>
        /// Get a builder for a specific object type
        /// </summary>
        static IBuilder GetBuilder(Type type)
        {
            // try to get an existing builder
            if (_Builders.TryGetValue(type, out IBuilder builder))
                return builder;
                
            var enumeratedType = ReflectionUtils.GetIEnumerableType(type);
            if (enumeratedType != null)
            {
                var innerBuilder = GetBuilder(enumeratedType);
                builder = BuildAnEnumerableBuilder(type, enumeratedType, innerBuilder);
            }
            else
            {
                builder = BuildABuilder(type);
            }

            // add to cache
            return _Builders.GetOrAdd(type, builder);
        }

        /// <summary>
        /// Create an IBuilder for a given enumerable type
        /// </summary>
        static IBuilder BuildAnEnumerableBuilder(Type type, Type enumeratedType, IBuilder innerBuilder)
        {
            // use reflection to create an instance of a
            // builder for the required type
            return (IBuilder)typeof(EnumerableBuilder<,>)
                .MakeGenericType(new[]{ type, enumeratedType })
                .GetConstructors()
                .Single(c => c.GetParameters().Length == 1)
                .Invoke(new object[] { innerBuilder });
        }

        /// <summary>
        /// Create an IBuilder for a given type
        /// </summary>
        static IBuilder BuildABuilder(Type type)
        {
            // use reflection to create an instance of a
            // builder for the required type
            return (IBuilder)typeof(Builder<>)
                .MakeGenericType(new[]{ type })
                .GetConstructors()
                .Single(c => c.GetParameters().Length == 0)
                .Invoke(new object[0]);
        }
    }
}
