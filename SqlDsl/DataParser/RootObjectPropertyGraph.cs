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
        /// If null, ignore. If not null, this object references a simple value
        /// </summary>
        readonly (int columnIndex, int rowNumberColumnIndex)? SimpleProperty;

        /// <summary>
        /// Specifies the type of this graph. If true graph only has one node which represents a simple value (e.g. a string, int etc...).
        /// Otherwise the graph represents a complex object with inner properties
        /// </summary>
        public bool IsSimpleValue => SimpleProperty.HasValue;

        /// <summary>
        /// If IsSimpleValue == true, will specify the column index of the simple value column
        /// Otherwise the value of this property should be ignored
        /// </summary>
        public int SimpleValueColumnIndex => SimpleProperty?.columnIndex ?? 0;

        /// <summary>
        /// If IsSimpleValue == true, will specify the row number column index for the SimpleValueColumn
        /// Otherwise the value of this property should be ignored
        /// </summary>
        public int SimpleValueRowNumberColumnIndex => SimpleProperty?.rowNumberColumnIndex ?? 0;

        /// <summary>
        /// Build an object graph
        /// </summary>
        /// <param name="colNames">The column names that this graph is based on.</param>
        /// <param name="simpleProps">Properties of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.</param>
        /// <param name="complexProps">Properties of an object which have sub properies</param>
        /// <param name="rowIdColumnNumbers">A composite of the row numbers which point to this object</param>
        public RootObjectPropertyGraph(
            IEnumerable<string> colNames,
            IEnumerable<(int index, string name, IEnumerable<int> rowNumberColumnIds, Type resultPropertyType, Type dataCellType)> simpleProps, 
            IEnumerable<(string name, ObjectPropertyGraph value)> complexProps, 
            IEnumerable<int> rowIdColumnNumbers)
            : base(simpleProps, complexProps, rowIdColumnNumbers)
        {
            ColumnNames = colNames.ToArray();
        }

        /// <summary>
        /// Build an object graph for a simple value.
        /// </summary>
        public RootObjectPropertyGraph(int columnIndex, int rowNumberColumnIndex)
            : this(Enumerable.Empty<string>(),
                Enumerable.Empty<(int, string, IEnumerable<int>, Type, Type)>(), 
                Enumerable.Empty<(string, ObjectPropertyGraph)>(), 
                Enumerable.Empty<int>())
        {
            SimpleProperty = (columnIndex, rowNumberColumnIndex);
        }

        public override string ToString() =>
            $"ColumnNames: [{ColumnNames.Select((c, i) => $"{i}-{c}").JoinString(", ")}]\n" +
            base.ToString();
    }
}
