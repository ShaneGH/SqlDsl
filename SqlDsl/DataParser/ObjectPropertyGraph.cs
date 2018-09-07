using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.DataParser
{
    /// <summary>
    /// Represents an object with it's property names
    /// </summary>
    class ObjectPropertyGraph
    {
        /// <summary>
        /// Properties of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.
        /// </summary>
        public readonly IEnumerable<(int index, string name, bool isEnumerable, IEnumerable<int> rowNumberColumnIds)> SimpleProps;
        
        /// <summary>
        /// Properties of an object which have sub properies
        /// </summary>
        public readonly IEnumerable<(string name, ObjectPropertyGraph value, bool isEnumerable)> ComplexProps;
        
        /// <summary>
        /// A composite of the row numbers which point to this object
        /// </summary>
        public readonly IEnumerable<int> RowNumberColumnIds;

        /// <summary>
        /// Build an object graph
        /// </summary>
        /// <param name="colNames">
        /// The names of the columns returned in the query.
        /// </param>
        /// <param name="rowNumberMap">A map from each column to the index of it's rown number column</param>
        /// <param name="objectType">The type of the object which this graph represents</param>
        public ObjectPropertyGraph(int[] rowNumberMap, IEnumerable<string> colNames, Type objectType)
            : this(rowNumberMap, colNames.Select((n, i) => (i, n.Split('.').ToArray())), objectType)
        {
        }

        /// <summary>
        /// Build an object graph
        /// </summary>
        /// <param name="colNames">
        /// The names of the columns returned in the query. Names are "." delimited to split a string into parts.
        /// </param>
        /// <param name="rowNumberMap">A map from each column to the index of it's rown number column</param>
        /// <param name="objectType">The type of the object which this graph represents</param>
        ObjectPropertyGraph(int[] rowNumberMap, IEnumerable<(int index, string[] name)> colNames, Type objectType)
        {
            var simpleProps = new List<(int index, string propertyName, bool isEnumerable, IEnumerable<int> rowNumberColumnIds)>();
            var complexProps = new List<(int index, string propertyName, IEnumerable<string> childProps, bool isEnumerable, Type propertyType)>();

            var typedColNames = objectType
                .GetFields()
                .Select(f => (name: f.Name, type: f.FieldType))
                .Concat(objectType
                    .GetProperties()
                    .Select(f => (name: f.Name, type: f.PropertyType)))
                .Enumerate();

            foreach (var col in colNames)
            {
                var colType = typedColNames
                    .Where(c => c.name == col.name[0])
                    .Select(c => c.type)
                    .FirstOrDefault();

                // property is not actually a member of this
                // object
                if (colType == null)
                    continue;

                var enumerableType = ReflectionUtils.GetIEnumerableType(colType);
                var isEnumerable = enumerableType != null;
                        
                // if there is only one name, the property belongs to this object
                if (col.name.Length == 1)
                {
                    // TODO: if a column has multiple row numbers (is a composite)?
                    simpleProps.Add((col.index, col.name[0], isEnumerable, new[]{ rowNumberMap[col.index] }));
                }
                // if there are more than one, the property belongs to a child of this object
                else if (col.name.Length > 1)
                {
                    var propertyType = enumerableType ?? colType;

                    // separate the property from this object (index == 0) from the properties of
                    // child objects
                    complexProps.Add((col.index, col.name[0], col.name.Skip(1), isEnumerable, propertyType));
                }
            }

            var cProps = complexProps
                // group properties by child property name
                .GroupBy(x => x.propertyName)
                // build child property graph
                .Select(group => (
                    group.Key, 
                    new ObjectPropertyGraph(rowNumberMap, group.Select(x => (x.index, x.childProps.ToArray())), group.First().propertyType),
                    group.First().isEnumerable
                ));
            
            SimpleProps = simpleProps;
            ComplexProps = cProps.Enumerate();
            
            // TODO: does ordering matter in a composite key?
            RowNumberColumnIds = SimpleProps
                .Where(sp => !sp.isEnumerable)
                .Select(sp => rowNumberMap[sp.index])
                .Distinct()
                .OrderBy(x => x)
                .Enumerate();
        }

        /// <summary>
        /// Get the unique id of a row, in the context of this object
        /// </summary>
        public string GetUniqueId(object[] row) => GetUniqueIdForSimpleProp(row, Enumerable.Empty<int>());

        /// <summary>
        /// Get the unique id of a row, in the context of a simple prop of this object
        /// </summary>
        public string GetUniqueIdForSimpleProp(object[] row, IEnumerable<int> simplePropRowNumberColumnIds)
        {
            // TODO: this method is used a lot.
            // Can results be cached somewhere?
            return RowNumberColumnIds
                .Concat(simplePropRowNumberColumnIds.OrEmpty())   
                .Select(r => row[r].ToString())
                .JoinString(";");
        }

        public override string ToString() =>
            SimpleProps
                .Select(p => $"{p.name}: {{ index: {p.index}, enumerable: {p.isEnumerable} }}")
                .Concat(ComplexProps.Select(p => $"{p.name}:\n  {p.value.ToString().Replace("\n", "\n  ")}"))
                .JoinString("\n");
    }
}