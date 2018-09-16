using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.DataParser
{
    //TODO: this class needs a cleanup

    /// <summary>
    /// Represents an object with it's property names
    /// </summary>
    public class ObjectPropertyGraph
    {
        /// <summary>
        /// Properties of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.
        /// </summary>
        public readonly IEnumerable<(int index, string name, IEnumerable<int> rowNumberColumnIds)> SimpleProps;
        
        /// <summary>
        /// Properties of an object which have sub properies
        /// </summary>
        public readonly IEnumerable<(string name, ObjectPropertyGraph value)> ComplexProps;
        
        /// <summary>
        /// A composite of the row numbers which point to this object
        /// </summary>
        public readonly IEnumerable<int> RowIdColumnNumbers;

        /// <summary>
        /// Build an object graph
        /// </summary>
        /// <param name="colNames">
        /// The names of the columns returned in the query.
        /// </param>
        /// <param name="rowNumberMap">A map from each column to the indexes of it's row number columns</param>
        /// <param name="objectType">The type of the object which this graph represents</param>
        /// <param name="propertyRowNumberColMap">A list of properties along with the row number cols that define their uniqueness</param>
        public ObjectPropertyGraph(int[][] rowNumberMap, IEnumerable<string> colNames, Type objectType, IEnumerable<(string name, IEnumerable<int> rowNumberCols)> propertyRowNumberColMap)
            : this(
                rowNumberMap, 
                colNames.Select((n, i) => (i, n.Split('.').ToArray())), 
                objectType, 
                Enumerable.Empty<int>(),
                propertyRowNumberColMap.Select(x => (x.name.Split('.').ToArray(), x.rowNumberCols)))
        {
        }

        /// <summary>
        /// Build an object graph
        /// </summary>
        /// <param name="colNames">
        /// The names of the columns returned in the query. Names are "." delimited to split a string into parts.
        /// </param>
        /// <param name="rowNumberMap">A map from each column to the indexes of it's row number columns</param>
        /// <param name="objectType">The type of the object which this graph represents</param>
        /// <param name="propertyRowNumberColMap">A list of properties along with the row number cols that define their uniqueness</param>
        ObjectPropertyGraph(
            int[][] rowNumberMap, 
            IEnumerable<(int index, string[] name)> colNames, 
            Type objectType, 
            IEnumerable<int> propertyRowNumberCols, 
            IEnumerable<(string[] name, IEnumerable<int> rowNumberCols)> propertyRowNumberColMap)
        {
            var simpleProps = new List<((int index, string propertyName, IEnumerable<int> rowNumberColumnIds) data, bool isEnumerable)>();
            var complexProps = new List<(int index, string propertyName, IEnumerable<string> childProps, Type propertyType)>();

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
                        
                // if there is only one name, the property belongs to this object
                if (col.name.Length == 1)
                {
                    // TODO: if a column has multiple row numbers (is a composite)?
                    var isEnumerable = enumerableType != null;
                    simpleProps.Add(((col.index, col.name[0], rowNumberMap[col.index]), isEnumerable));
                }
                // if there are more than one, the property belongs to a child of this object
                else if (col.name.Length > 1)
                {
                    var propertyType = enumerableType ?? colType;

                    // separate the property from this object (index == 0) from the properties of
                    // child objects
                    complexProps.Add((col.index, col.name[0], col.name.Skip(1), propertyType));
                }
            }

            var cProps = complexProps
                // group properties by child property name
                .GroupBy(x => x.propertyName)
                // build child property graph
                .Select(group => 
                {
                    var gp = group.Enumerate();
                    var rowNumberCols = propertyRowNumberColMap
                        .Where(r => r.name.Length > 0 && r.name[0] == group.Key)
                        .Enumerate();

                    var forThisObj = rowNumberCols
                        .Where(c => c.name.Length == 1)
                        .SelectMany(c => c.rowNumberCols);

                    var forChildren = rowNumberCols
                        .Where(c => c.name.Length > 1)
                        .Select(c => (c.name.Skip(1).ToArray(), c.rowNumberCols));

                    return (
                        group.Key, 
                        new ObjectPropertyGraph(
                            rowNumberMap, 
                            gp.Select(x => (x.index, x.childProps.ToArray())), 
                            gp.First().propertyType,
                            forThisObj,
                            forChildren)
                    );
                });
            
            SimpleProps = simpleProps
                .Select(p => p.data)
                .Enumerate();
                
            ComplexProps = cProps.Enumerate();
            
            // TODO: does ordering matter in a composite key?
            RowIdColumnNumbers = simpleProps
                .Where(sp => !sp.isEnumerable)
                .SelectMany(sp => rowNumberMap[sp.data.index])
                .Concat(propertyRowNumberCols)
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
            // Can results be cached or more efficient method used?
            return RowIdColumnNumbers
                .Concat(simplePropRowNumberColumnIds.OrEmpty())   
                .Select(r => row[r].ToString())
                .JoinString(";");
        }

        public override string ToString() =>
            $"RowNumberColumnIds: [{RowIdColumnNumbers.JoinString(",")}]\n" +
            SimpleProps
                .Select(p => $"{p.name}: {{ index: {p.index} }}")
                .Concat(ComplexProps.Select(p => $"{p.name}:\n  {p.value.ToString().Replace("\n", "\n  ")}"))
                .JoinString("\n");
    }
}