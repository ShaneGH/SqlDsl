using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.ObjectBuilders;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.DataParser
{
    // TODO; col names not necessary. Should use rows and cols as separate objects
    using Row = System.Collections.Generic.List<(string key, object value)>;

    /// <summary>
    /// Utils to parse query results
    /// </summary>
    public static class Parser
    {
        /// <summary>
        /// Parse the results of a sql query
        /// </summary>
        /// <param name="rows">The query results</param>
        /// <param name="primaryTable">The table from the [FROM] query clause. If null, will assume that there is a column named "##rowid" to group results by.</param>
        public static IEnumerable<TResult> Parse<TResult>(this IEnumerable<Row> rows, string primaryTable = null) =>
            _Parse<TResult>(rows, primaryTable).Enumerate();

        /// <summary>
        /// Parse the results of a sql query
        /// </summary>
        /// <param name="rows">The query results</param>
        /// <param name="primaryTable">The table from the [FROM] query clause. If null, will assume that there is a column named "##rowid" to group results by.</param>
        static IEnumerable<TResult> _Parse<TResult>(this IEnumerable<Row> rows, string primaryTable)
        {
            // group the results by the primary (SELECT) table
            var resultGroups = rows
                .OrEmpty()
                .GroupBy(r => r.ColumnId(primaryTable))
                .Select(IEnumerableUtils.Enumerate)
                .Enumerate();

            if (!resultGroups.Any())
                yield break;

            // build an object graph from the column names
            var colNames = resultGroups
                .First()
                .First()
                .Select((cell, i) => (i, cell.key))
                .Enumerate();

            var objectGraph = new ObjectPropertyGraph(colNames);

            // return a new object for each group of rows
            foreach (var results in resultGroups)
                yield return (TResult)Builders.Build(
                    typeof(TResult), 
                    CreateObject(objectGraph, results).First());
        }
        
        /// <summary>
        /// Get the id of the current column. The idPrefix will point to the primary (SELECT FROM) table
        /// </summary>
        static long ColumnId(this Row row, string idPrefix = null)
        {
            var rowId = idPrefix == null ? SqlBuilderBase.RowIdName : $"{idPrefix}.{SqlBuilderBase.RowIdName}";

            foreach (var col in row)
                if (col.Item1 == rowId)
                    return Convert.ToInt64(col.Item2);

            throw new InvalidOperationException($"Cannot find row id for table {idPrefix}.");
        }
        
        /// <summary>
        /// Map a group of rows to an object property graph to an object graph with properties
        /// </summary>
        static IEnumerable<ObjectGraph> CreateObject(ObjectPropertyGraph propertyGraph, IEnumerable<Row> rows)
        {
            // Ensure IEnumerable is not enumerated multiple times
            rows = rows.Enumerate();

            if (!rows.Any())
                yield break;

            // Get the id of the column with the row number for this object
            var rowNumberColumn = propertyGraph
                .SimpleProps
                .Where(c => c.name == SqlBuilderBase.RowIdName)
                .Select(x => (int?)x.index)
                .FirstOrDefault();

            var ids = new HashSet<long>();
            foreach (var row in rows)
            {
                // if there is no row number, assume that all rows point to one object
                var rowNumber = rowNumberColumn != null ? 
                    Convert.ToInt64(row[rowNumberColumn.Value].value) : 
                    -1;

                // This row is a duplicate
                if (!ids.Add(rowNumber))
                    continue;

                yield return new ObjectGraph
                {
                    // simple prop values can be found by their column index
                    SimpleProps = propertyGraph.SimpleProps
                        .Select(p => (p.name, new [] { row[p.index].value }.Skip(0)))
                        .Enumerate(),
                    // complex prop values are build recursively
                    ComplexProps = propertyGraph.ComplexProps
                        .Select(p => (p.name, CreateObject(p.value, rows)))
                        .Enumerate()
                };

                // Early out clause. Performs the same job as
                // hashset check above
                if (rowNumberColumn == null)
                    break;
            }
        }
    }
}
