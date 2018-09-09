using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.DataParser
{
    /// <summary>
    /// Represents the root of a graph of an object with it's property names
    /// </summary>
    class RootObjectPropertyGraph : ObjectPropertyGraph
    {
        /// <summary>
        /// The column names that this graph is based on
        /// </summary>
        public readonly string[] ColumnNames;

        /// <summary>
        /// Build an object graph
        /// </summary>
        /// <param name="colNames">
        /// The names of the columns returned in the query.
        /// </param>
        /// <param name="rowNumberMap">A map from each column to the indexes of it's row number columns</param>
        /// <param name="objectType">The type of the object which this graph represents</param>
        /// <param name="propertyRowNumberColMap">A list of properties along with the row number cols that define their uniqueness</param>
        public RootObjectPropertyGraph(int[][] rowNumberMap, IEnumerable<string> colNames, Type objectType, IEnumerable<(string name, IEnumerable<int> rowNumberCols)> propertyRowNumberColMap)
            : base(rowNumberMap, colNames, objectType, propertyRowNumberColMap)
        {
            ColumnNames = colNames.ToArray();
        }

        public override string ToString() =>
            $"ColumnNames: [{ColumnNames.Select((c, i) => $"{i}-{c}").JoinString(", ")}]\n" +
            base.ToString();
    }
}
