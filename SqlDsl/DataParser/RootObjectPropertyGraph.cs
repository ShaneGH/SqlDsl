using SqlDsl.Utils;
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
        public RootObjectPropertyGraph(IEnumerable<string> colNames)
            : base(colNames)
        {
            ColumnNames = colNames.ToArray();
        }
    }
}
