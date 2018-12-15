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
        public override ObjectPropertyGraph PropertyGraph => GetPropertyGraph();

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

        public ReusableObjectGraph Clone()
        {
            var g = Cache.GetGraph(Logger);
            g.Init(_PropertyGraph, _Objects);
            return g;
        }

        ObjectPropertyGraph GetPropertyGraph()
        {
            AssertObjectInit();
            return _PropertyGraph;
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
            if (_PropertyGraph == null || _Objects == null)
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
}