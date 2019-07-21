using SqlDsl.DataParser;
using SqlDsl.Utils;
using SqlDsl.Utils.EqualityComparers;
using SqlDsl.Utils.ObjectCaches;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// A generic object graph which can be converted into a concrete class
    /// </summary>
    public class ObjectGraph : ReusableObject
    {
        /// <summary>
        /// The type of the constructor args to be used with this object
        /// </summary>
        public virtual Type[] ConstructorArgTypes => PropertyGraph.ConstructorArgTypes;

        public ObjectPropertyGraph PropertyGraph { get; private set; }

        public IEnumerable<object[]> Objects { get; private set; }

        internal readonly IReleasableCache<ObjectGraph> Cache;

        internal ObjectGraph(IReleasableCache<ObjectGraph> cache)
            : base(cache)
        {
            Cache = cache;
        }
        
        public void Init(ObjectPropertyGraph propertyGraph, IEnumerable<object[]> objects)
        {
            PropertyGraph = propertyGraph ?? throw new ArgumentNullException(nameof(propertyGraph));
            Objects = objects ?? throw new ArgumentNullException(nameof(objects));
        }

        protected override void _Dispose()
        {
            PropertyGraph = null;
            Objects = null;
        }

        public ObjectGraph Clone()
        {
            var clone = Cache.ReleseOrCreateItem();
            clone.Init(PropertyGraph, Objects);
            return clone;
        }
        
        /// <summary>
        /// Simple properties such as int, string, List&lt;int>, List&lt;string> etc...
        /// </summary>
        public virtual IEnumerable<(string name, IEnumerable<object> value, bool isEnumerableDataCell)> GetSimpleProps() =>
            PropertyGraph.SimpleProps.Select(GetSimpleProp);

        /// <summary>
        /// Complex properties will have properties of their own
        /// </summary>
        public virtual IEnumerable<(string name, IEnumerable<ObjectGraph> value)> GetComplexProps() =>
            PropertyGraph.ComplexProps
                .Select(p => (p.name, CreateObject(p.value, Objects)));

        /// <summary>
        /// Simple constructor args such as int, string, List&lt;int>, List&lt;string> etc...
        /// </summary>
        public virtual IEnumerable<(int argIndex, IEnumerable<object> value, bool isEnumerableDataCell)> GetSimpleConstructorArgs() =>
            PropertyGraph.SimpleConstructorArgs.Select(GetSimpleCArg);

        /// <summary>
        /// Complex constructor args will have properties of their own
        /// </summary>
        public virtual IEnumerable<(int argIndex, IEnumerable<ObjectGraph> value)> GetComplexConstructorArgs() =>
            PropertyGraph.ComplexConstructorArgs
                .Select(p => (p.argIndex, CreateObject(p.value, Objects)));

        (int argIndex, IEnumerable<object> value, bool isEnumerableDataCell) GetSimpleCArg(
                    (int index, int argIndex, int[] primaryKeyColumns, Type resultPropertyType, Type dataCellType) p)
        {
            var (data, cellEnumType) = GetSimpleDataAndType(p.index, p.primaryKeyColumns, p.dataCellType);
            return (p.argIndex, data, cellEnumType != null);
        }

        (string name, IEnumerable<object> value, bool isEnumerableDataCell) GetSimpleProp((int index, string name, int[] primaryKeyColumns, Type resultPropertyType, Type dataCellType) p)
        {
            var (data, cellEnumType) = GetSimpleDataAndType(p.index, p.primaryKeyColumns, p.dataCellType);
            return (p.name, data, cellEnumType != null);
        }

        (IEnumerable<object> value, Type cellEnumType) GetSimpleDataAndType(int index, int[] primaryKeyColumns, Type dataCellType)
        {
            var dataRowsForProp = PropertyGraph.GetDataRowsForSimpleProperty(Objects, primaryKeyColumns);

            var data = dataRowsForProp
                .Select(o => o[index])
                .ToArray();

            var cellEnumType = dataCellType == null ?
                null :
                ReflectionUtils.GetIEnumerableType(dataCellType);

            return (data, cellEnumType);
        }

        IEnumerable<ObjectGraph> CreateObject(ObjectPropertyGraph propertyGraph, IEnumerable<object[]> rows)
        {
            var objectsData = propertyGraph.GroupAndFilterData(rows);

            foreach (var obj in objectsData)
            {
                var graph = Cache.ReleseOrCreateItem();
                graph.Init(propertyGraph, obj);
                yield return graph;
            }
        }

        public override string ToString()
        {
            var simple = GetSimpleProps()
                .OrEmpty()
                .Select(ps => $"S_{ps.name}:\n  [{ps.value.Select(p => $"\n    {p}").JoinString("")}\n  ]")
                .JoinString("\n");

            var complex = GetComplexProps()
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
    }
}