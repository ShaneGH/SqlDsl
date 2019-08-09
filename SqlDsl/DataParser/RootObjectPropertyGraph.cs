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
        private static readonly SimpleProp[] DefaultSimpleProps = new SimpleProp[0];

        /// <summary>
        /// The column names that this graph is based on
        /// </summary>
        public readonly string[] ColumnNames;

        /// <summary>
        /// If null, ignore. If not null, this object references a simple value
        /// </summary>
        readonly (int columnIndex, int primaryKeyColumnIndex, Type cellType, bool simplePropertyCellIsTypeEnumerable)? SimpleProperty;

        /// <summary>
        /// If IsSimpleValue == true, will specify the column index of the simple value column
        /// Otherwise the value of this property should be ignored
        /// </summary>
        public int SimpleValueColumnIndex => SimpleProperty?.columnIndex ?? 0;

        /// <summary>
        /// If IsSimpleValue == true, will specify the row number column index for the SimpleValueColumn
        /// Otherwise the value of this property should be ignored
        /// </summary>
        public int SimpleValuePrimaryKeyColumn => SimpleProperty?.primaryKeyColumnIndex ?? 0;

        /// <summary>
        /// If IsSimpleValue == true, will specify the type of the cell
        /// Otherwise the value of this property should be ignored
        /// </summary>
        public Type SimplePropertyCellType => SimpleProperty?.cellType;

        /// <summary>
        /// Build an object graph
        /// </summary>
        /// <param name="objectType">The type of the object.</param>
        /// <param name="colNames">The column names that this graph is based on.</param>
        /// <param name="simpleProps">Properties of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.</param>
        /// <param name="complexProps">Properties of an object which have sub properies</param>
        /// <param name="primaryKeyColumns">A composite of the row numbers which point to this object</param>
        public RootObjectPropertyGraph(
            Type objectType,
            IEnumerable<string> colNames,
            SimpleProp[] simpleProps, 
            ComplexProp[] complexProps, 
            int[] primaryKeyColumns,
            SimpleConstructorArg[] simpleConstructorArgs = null,
            ComplexConstructorArg[] complexConstructorArgs = null)
            : base(objectType, simpleProps, complexProps, primaryKeyColumns, simpleConstructorArgs, complexConstructorArgs)
        {
            ColumnNames = colNames.ToArray();
        }

        /// <summary>
        /// Build an object graph for a simple value.
        /// </summary>
        /// <param name="objectType">The type of the object.</param>
        public RootObjectPropertyGraph(Type objectType, int columnIndex, int primaryKeyColumnIndex, Type cellType, bool cellTypeIsEnumerable)
            : this(objectType, Enumerable.Empty<string>(), null,  null,  null)
        {
            SimpleProperty = (columnIndex, primaryKeyColumnIndex, cellType, cellTypeIsEnumerable);
        }

        public override string ToString() =>
            $"ColumnNames: [{ColumnNames.Select((c, i) => $"{i}-{c}").JoinString(", ")}]\n" +
            base.ToString();
    }
}
