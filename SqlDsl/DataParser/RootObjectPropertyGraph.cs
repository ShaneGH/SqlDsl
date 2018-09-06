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
        /// <param name="rowNumberMap">A map from each column to the index of it's rown number column</param>
        /// <param name="objectType">The type of the object which this graph represents</param>
        public RootObjectPropertyGraph(int[] rowNumberMap, IEnumerable<string> colNames, Type objectType)
            : base(rowNumberMap, colNames, objectType)
        {
            ColumnNames = colNames.ToArray();
        }
    }
}
