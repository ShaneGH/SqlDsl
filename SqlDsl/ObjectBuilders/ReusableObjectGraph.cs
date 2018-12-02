using SqlDsl.DataParser;
using SqlDsl.Utils;
using SqlDsl.Utils.EqualityComparers;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// Object graph which can be re-used via Dispose and Init methods
    /// </summary>
    public class ReusableObjectGraph : ObjectGraph<ReusableObjectGraph>, IDisposable
    {
        /// <summary>
        /// The cache that this object belongs to. Calling dispose will return this object to it's cache
        /// </summary>
        internal readonly IObjectGraphCache Cache;

        private readonly ILogger Logger;

        private ObjectPropertyGraph _PropertyGraph;
        public override ObjectPropertyGraph PropertyGraph => _PropertyGraph;

        private IEnumerable<object[]> _Objects;
        public override IEnumerable<object[]> Objects => _Objects;

        internal ReusableObjectGraph(IObjectGraphCache cache, ILogger logger)
        {
            Cache = cache ?? throw new ArgumentNullException(nameof(cache));
            Logger = logger;
            
            if (logger.CanLogDebug(LogMessages.CreatedObjectGraphAllocation))
                logger.LogDebug("Object graph created", LogMessages.CreatedObjectGraphAllocation);
        }

        public ReusableObjectGraph(ILogger logger = null)
            : this(PassthroughCache.Instance, logger)
        {
        }
        
        public void Init(ObjectPropertyGraph propertyGraph, IEnumerable<object[]> objects)
        {
            _PropertyGraph = propertyGraph ?? throw new ArgumentNullException(nameof(propertyGraph));
            _Objects = objects ?? throw new ArgumentNullException(nameof(objects));
        }

        /// <inheritdoc />
        public override IEnumerable<(string name, IEnumerable<object> value, bool isEnumerableDataCell)> GetSimpleProps()
        {
            AssertObjectInit();
            return base.GetSimpleProps();
        }

        /// <inheritdoc />
        public override IEnumerable<(string name, IEnumerable<ReusableObjectGraph> value)> GetComplexProps()
        {
            AssertObjectInit();
            return base.GetComplexProps();
        }

        /// <inheritdoc />
        public override IEnumerable<(int argIndex, IEnumerable<object> value, bool isEnumerableDataCell)> GetSimpleConstructorArgs()
        {
            AssertObjectInit();
            return base.GetSimpleConstructorArgs();
        }

        /// <inheritdoc />
        public override IEnumerable<(int argIndex, IEnumerable<ReusableObjectGraph> value)> GetComplexConstructorArgs()
        {
            AssertObjectInit();
            return base.GetComplexConstructorArgs();
        }

        void AssertObjectInit()
        {   
            if (PropertyGraph == null || Objects == null)
                throw new InvalidOperationException("This object has been not been initialized yet.");
        }

        protected override ReusableObjectGraph BuildChildGraph(ObjectPropertyGraph propertyGraph, IEnumerable<object[]> rows)
        {
            var graph = Cache.GetGraph(Logger);
            graph.Init(propertyGraph, rows);

            return graph;
        }

        public void Dispose()
        {
            _PropertyGraph = null;
            _Objects = null;
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

            public ReusableObjectGraph GetGraph(ILogger logger)
            {
                return new ReusableObjectGraph(this, logger);
            }

            public void ReleaseGraph(ReusableObjectGraph graph)
            {
            }
        }
    }
    
    // /// <summary>
    // /// A generic object graph which can be converted into a concrete class
    // /// </summary>
    // public class ObjectGraph_Old : IDisposable
    // {
    //     //TODO: test allocations for simplest of SELECT queries (no maps and no joins)
    //     // I have a suspicion that there are a lot of allocations

    //     /// <summary>
    //     /// Simple properties such as int, string, List&lt;int>, List&lt;string> etc...
    //     /// </summary>
    //     public IEnumerable<(string name, IEnumerable<object> value, bool isEnumerableDataCell)> SimpleProps;
        
    //     /// <summary>
    //     /// Complex properties will have properties of their own
    //     /// </summary>
    //     public Func<IEnumerable<(string name, IEnumerable<ObjectGraph_Old> value)>> BuildComplexProps;
        
    //     /// <summary>
    //     /// Simple constructor args such as int, string, List&lt;int>, List&lt;string> etc...
    //     /// </summary>
    //     public IEnumerable<(int argIndex, IEnumerable<object> value, bool isEnumerableDataCell)> SimpleConstructorArgs;
        
    //     /// <summary>
    //     /// Complex constructor args will have properties of their own
    //     /// </summary>
    //     public Func<IEnumerable<(int argIndex, IEnumerable<ObjectGraph_Old> value)>> BuildComplexConstructorArgs;

    //     /// <summary>
    //     /// The type of the constructor args to be used with this object
    //     /// </summary>
    //     public Type[] ConstructorArgTypes;

    //     /// <summary>
    //     /// The cache that this object belongs to. Calling dispose will return this object to it's cache
    //     /// </summary>
    //     internal readonly IObjectGraphCache Cache;

    //     public ObjectGraph_Old()
    //         : this(PassthroughCache.Instance, null)
    //     {
    //     }

    //     internal ObjectGraph_Old(IObjectGraphCache cache, ILogger logger)
    //     {
    //         Cache = cache;
    //         SetDefaults();
            
    //         if (logger.CanLogDebug(LogMessages.CreatedObjectGraphAllocation))
    //             logger.LogDebug("Object graph created", LogMessages.CreatedObjectGraphAllocation);
    //     }

    //     private static readonly IEnumerable<(string, IEnumerable<object>, bool)> DefaultSimpleProps = Enumerable.Empty<(string, IEnumerable<object>, bool)>();
    //     private static readonly Func<IEnumerable<(string, IEnumerable<ObjectGraph_Old>)>> DefaultBuildComplexProps = () => Enumerable.Empty<(string, IEnumerable<ObjectGraph_Old>)>();
    //     private static readonly IEnumerable<(int, IEnumerable<object>, bool)> DefaultSimpleConstructorArgs = Enumerable.Empty<(int, IEnumerable<object>, bool)>();
    //     private static readonly Func<IEnumerable<(int, IEnumerable<ObjectGraph_Old>)>> DefaultBuildComplexConstructorArgs = () => Enumerable.Empty<(int, IEnumerable<ObjectGraph_Old>)>();
    //     private static readonly Type[] DefaultConstructorArgTypes = new Type[0];

    //     protected void SetDefaults()
    //     {
    //         SimpleProps = DefaultSimpleProps;
    //         BuildComplexProps = DefaultBuildComplexProps;
    //         SimpleConstructorArgs = DefaultSimpleConstructorArgs;
    //         BuildComplexConstructorArgs = DefaultBuildComplexConstructorArgs;
    //         ConstructorArgTypes = DefaultConstructorArgTypes;
    //     }
    // }
}