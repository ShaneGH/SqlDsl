using SqlDsl.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// A generic object graph which can be converted into a concrete class
    /// </summary>
    public class ObjectGraph : IDisposable
    {
        //TODO: test allocations for simplest of SELECT queries (no maps and no joins)
        // I have a suspicion that there are a lot of allocations

        /// <summary>
        /// Simple properties such as int, string, List&lt;int>, List&lt;string> etc...
        /// </summary>
        public IEnumerable<(string name, IEnumerable<object> value, bool isEnumerableDataCell)> SimpleProps;
        
        /// <summary>
        /// Complex properties will have properties of their own
        /// </summary>
        public Func<IEnumerable<(string name, IEnumerable<ObjectGraph> value)>> BuildComplexProps;
        
        /// <summary>
        /// Simple constructor args such as int, string, List&lt;int>, List&lt;string> etc...
        /// </summary>
        public IEnumerable<(int argIndex, IEnumerable<object> value, bool isEnumerableDataCell)> SimpleConstructorArgs;
        
        /// <summary>
        /// Complex constructor args will have properties of their own
        /// </summary>
        public Func<IEnumerable<(int argIndex, IEnumerable<ObjectGraph> value)>> BuildComplexConstructorArgs;

        /// <summary>
        /// The type of the constructor args to be used with this object
        /// </summary>
        public Type[] ConstructorArgTypes;

        /// <summary>
        /// The cache that this object belongs to. Calling dispose will return this object to it's cache
        /// </summary>
        internal readonly IObjectGraphCache Cache;

        public ObjectGraph()
            : this(PassthroughCache.Instance, null)
        {
        }

        internal ObjectGraph(IObjectGraphCache cache, ILogger logger)
        {
            Cache = cache;
            SetDefaults();
            
            if (logger.CanLogDebug(LogMessages.CreatedObjectGraphAllocation))
                logger.LogDebug("Object graph created", LogMessages.CreatedObjectGraphAllocation);
        }

        private static readonly IEnumerable<(string, IEnumerable<object>, bool)> DefaultSimpleProps = Enumerable.Empty<(string, IEnumerable<object>, bool)>();
        private static readonly Func<IEnumerable<(string, IEnumerable<ObjectGraph>)>> DefaultBuildComplexProps = () => Enumerable.Empty<(string, IEnumerable<ObjectGraph>)>();
        private static readonly IEnumerable<(int, IEnumerable<object>, bool)> DefaultSimpleConstructorArgs = Enumerable.Empty<(int, IEnumerable<object>, bool)>();
        private static readonly Func<IEnumerable<(int, IEnumerable<ObjectGraph>)>> DefaultBuildComplexConstructorArgs = () => Enumerable.Empty<(int, IEnumerable<ObjectGraph>)>();
        private static readonly Type[] DefaultConstructorArgTypes = new Type[0];

        protected void SetDefaults()
        {
            SimpleProps = DefaultSimpleProps;
            BuildComplexProps = DefaultBuildComplexProps;
            SimpleConstructorArgs = DefaultSimpleConstructorArgs;
            BuildComplexConstructorArgs = DefaultBuildComplexConstructorArgs;
            ConstructorArgTypes = DefaultConstructorArgTypes;
        }

        public override string ToString()
        {
            var simple = SimpleProps
                .OrEmpty()
                .Select(ps => $"S_{ps.name}:\n  [{ps.value.Select(p => $"\n    {p}").JoinString("")}\n  ]")
                .JoinString("\n");

            var complex = BuildComplexProps()
                .OrEmpty()
                .Select(ps => 
                {
                    var propStrings = ps.value
                        .Select(p => $"{p.ToString().Replace("\n", "\n    ")}")
                        .JoinString("");

                    return $"C_{ps.name}:\n  {{\n    {propStrings}\n  }}";
                })
                .JoinString("\n");

            return $"{simple}\n{complex}";
        }

        public void Dispose()
        {
            SetDefaults();
            if (Cache != null)
                Cache.ReleaseGraph(this);
        }

        /// <summary>
        /// A cache which does nothing. Is used for objects with no cache
        /// </summary>
        private class PassthroughCache : IObjectGraphCache
        {
            public static PassthroughCache Instance = new PassthroughCache();

            private PassthroughCache() 
            {
            }

            public ObjectGraph GetGraph(ILogger logger)
            {
                return new ObjectGraph(this, logger);
            }

            public void ReleaseGraph(ObjectGraph graph)
            {
            }
        }
    }
}