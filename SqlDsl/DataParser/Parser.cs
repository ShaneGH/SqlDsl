using SqlDsl.ObjectBuilders;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.DataParser
{
    /// <summary>
    /// Utils to parse query results
    /// </summary>
    public static class Parser
    {
        /// <summary>
        /// Parse the results of a sql query
        /// </summary>
        /// <param name="rows">The query results</param>
        /// <param name="columnNames">The query columns</param>
        /// <param name="rowNumberMap">A map from each column to the indexes of it's row number columns</param>
        /// <param name="primaryRowIdColumnNumber">The index of the column which contains the primary (non duplicatable) row number.</param>
        public static IEnumerable<TResult> Parse<TResult>(this IEnumerable<object[]> rows, IEnumerable<string> columnNames, int[][] rowNumberMap, int primaryRowIdColumnNumber) =>
            Parse<TResult>(rows, new RootObjectPropertyGraph(rowNumberMap, columnNames, typeof(TResult)), rowNumberMap, primaryRowIdColumnNumber);
            
        /// <summary>
        /// Parse the results of a sql query
        /// </summary>
        /// <param name="rows">The query results</param>
        /// <param name="propertyGraph">The query columns mapped to an object graph</param>
        /// <param name="rowNumberMap">A map from each column to the indexes of it's row number columns</param>
        /// <param name="primaryRowIdColumnNumber">The index of the column which contains the primary (non duplicatable) row number.</param>
        internal static IEnumerable<TResult> Parse<TResult>(this IEnumerable<object[]> rows, RootObjectPropertyGraph propertyGraph, int[][] rowNumberMap, int primaryRowIdColumnNumber) =>
            _Parse<TResult>(rows, propertyGraph, rowNumberMap, primaryRowIdColumnNumber).Enumerate();

        /// <summary>
        /// Parse the results of a sql query
        /// </summary>
        /// <param name="rows">The query results</param>
        /// <param name="propertyGraph">The query columns mapped to an object graph</param>
        /// <param name="rowNumberMap">A map from each column to the indexes of it's row number columns</param>
        /// <param name="primaryRowIdColumnNumber">The index of the column which contains the primary (non duplicatable) row number.</param>
        static IEnumerable<TResult> _Parse<TResult>(this IEnumerable<object[]> rows, RootObjectPropertyGraph propertyGraph, int[][] rowNumberMap, int primaryRowIdColumnNumber)
        {
            foreach (var row in rows.GroupBy(r => r[primaryRowIdColumnNumber]))
                foreach (var objValues in CreateObject(propertyGraph, rowNumberMap, row.ToEnumerable()))
                    yield return (TResult)Builders.Build(typeof(TResult), objValues);
        }

        /// <summary>
        /// Map a group of rows to an object property graph to an object graph with properties
        /// </summary>
        /// <param name="objects">A raw block of data, which has not been grouped into objects</param>
        /// <param name="rowNumberMap">A map from each column to the indexes of it's rown number columns</param>
        static IEnumerable<ObjectGraph> CreateObject(ObjectPropertyGraph propertyGraph, int[][] rowNumberMap, IEnumerable<object[]> rows)
        {
            // group the data into individual objects, where an object has multiple rows (for sub properties which are enumerable)
            var objectsData = rows.GroupBy(r => 
                propertyGraph.RowNumberColumnIds.Select(i => r[i]).ToArray(), 
                ArrayComparer<object>.Instance);

            return CreateObject(propertyGraph, rowNumberMap, objectsData);
        }

        /// <summary>
        /// Map a group of rows to an object property graph to an object graph with properties
        /// </summary>
        /// <param name="objects">An enumerable of objects. Each object can span multiple rows (corresponding to sub properties which are enumerable)</param>
        /// <param name="rowNumberMap">A map from each column to the indexes of it's rown number columns</param>
        static IEnumerable<ObjectGraph> CreateObject(ObjectPropertyGraph propertyGraph, int[][] rowNumberMap, IEnumerable<IEnumerable<object[]>> objects)
        {
            foreach (var objectData in objects)
            {
                yield return new ObjectGraph
                {
                    // simple prop values can be found by their column index
                    SimpleProps = propertyGraph.SimpleProps
                        .Select(GetSimpleProp)
                        .Enumerate(),
                    // complex prop values are build recursively
                    ComplexProps = propertyGraph.ComplexProps
                        .Select(p => (p.name, CreateObject(p.value, rowNumberMap, objectData).Enumerate()))
                        .Enumerate()
                };

                (string name, IEnumerable<object> value) GetSimpleProp((int index, string name, IEnumerable<int> rowNumberColumnIds) p)
                {
                    return (p.name, objectData
                        // run a "Distinct" on the rowNumbers
                        .GroupBy(d => propertyGraph.GetUniqueIdForSimpleProp(d, p.rowNumberColumnIds))
                        .Select(Enumerable.First)
                        .Select(o => o[p.index]));
                }
            }
        }
    }
}
