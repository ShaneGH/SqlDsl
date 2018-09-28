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
        //TODO: change IEnumerables to arrays and optimize code usage

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
        /// <param name="simpleProps">Properties of an object with simple values like strings, ints etc... The index is the index of the column in the sql query resuts table.</param>
        /// <param name="complexProps">Properties of an object which have sub properies</param>
        /// <param name="rowIdColumnNumbers">A composite of the row numbers which point to this object</param>
        public ObjectPropertyGraph(
            IEnumerable<(int index, string name, IEnumerable<int> rowNumberColumnIds)> simpleProps, 
            IEnumerable<(string name, ObjectPropertyGraph value)> complexProps, 
            IEnumerable<int> rowIdColumnNumbers)
        {
            SimpleProps = simpleProps.OrEmpty();
            ComplexProps = complexProps.OrEmpty();
            RowIdColumnNumbers = rowIdColumnNumbers.OrEmpty();
        }

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
                .Select(p => $"{p.name}: {{ index: {p.index}, rids: [{p.rowNumberColumnIds.JoinString(",")}] }}")
                .Concat(ComplexProps.Select(p => $"{p.name}:\n  {p.value.ToString().Replace("\n", "\n  ")}"))
                .JoinString("\n");
    }
}