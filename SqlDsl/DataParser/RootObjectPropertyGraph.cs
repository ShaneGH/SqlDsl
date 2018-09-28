using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.DataParser
{
    /// <summary>
    /// Represents the root of a graph of an object with it's property names
    /// </summary>
    public class RootObjectPropertyGraph : ObjectPropertyGraph
    {
        /// <summary>
        /// The column names that this graph is based on
        /// </summary>
        public readonly string[] ColumnNames;

        /// <summary>
        /// Build an object graph
        /// </summary>
        /// <param name="colNames">The column names that this graph is based on.</param>
        /// <param name="simpleProps">Properties of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.</param>
        /// <param name="complexProps">Properties of an object which have sub properies</param>
        /// <param name="rowIdColumnNumbers">A composite of the row numbers which point to this object</param>
        public RootObjectPropertyGraph(
            IEnumerable<string> colNames,
            IEnumerable<(int index, string name, IEnumerable<int> rowNumberColumnIds)> simpleProps, 
            IEnumerable<(string name, ObjectPropertyGraph value)> complexProps, 
            IEnumerable<int> rowIdColumnNumbers)
            : base(simpleProps, complexProps, rowIdColumnNumbers)
        {
            ColumnNames = colNames.ToArray();
        }

        public override string ToString() =>
            $"ColumnNames: [{ColumnNames.Select((c, i) => $"{i}-{c}").JoinString(", ")}]\n" +
            base.ToString();
    }
}
