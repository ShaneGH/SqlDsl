using System;
using System.Collections.Concurrent;
using System.Linq;

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
        public static object Build(Type type, ObjectGraph values)
        {
            // try to get an existing builder
            if (!_Builders.TryGetValue(type, out IBuilder builder))
            {
                // use reflection to create an instance of a
                // builder for the required type
                builder = (IBuilder)typeof(Builder<>)
                    .MakeGenericType(new[]{ type })
                    .GetConstructors()
                    .Single(c => c.GetParameters().Length == 0)
                    .Invoke(new object[0]);

                // add to cache
                _Builders.TryAdd(type, builder);
            }

            // use cached builder to create concrete object
            return builder.Build(values);
        }
    }
}
