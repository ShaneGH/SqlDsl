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
        /// <param name="propertyGraph">The query columns mapped to an object graph</param>
        internal static IEnumerable<TResult> Parse<TResult>(this IEnumerable<object[]> rows, RootObjectPropertyGraph propertyGraph) =>
            _Parse<TResult>(rows, propertyGraph).Enumerate();

        /// <summary>
        /// Parse the results of a sql query
        /// </summary>
        /// <param name="rows">The query results</param>
        /// <param name="propertyGraph">The query columns mapped to an object graph</param>
        static IEnumerable<TResult> _Parse<TResult>(this IEnumerable<object[]> rows, RootObjectPropertyGraph propertyGraph)
        {
            if (propertyGraph.IsSimpleValue)
            {
                // group the data into individual objects, where an object has multiple rows (for sub properties which are enumerable)
                var values = rows
                    .GroupBy(r => r[propertyGraph.SimpleValueRowNumberColumnIndex])
                    .Select(r => r.First()[propertyGraph.SimpleValueColumnIndex]);

                var convertor = TypeConvertors.GetConvertor<TResult>();
                foreach (var value in values)
                    yield return convertor(value);
            }

            foreach (var obj in CreateObject(propertyGraph, rows))
                yield return (TResult)Builders.Build(typeof(TResult), obj);
        }

        /// <summary>
        /// Map a group of rows to an object property graph to an object graph with properties
        /// </summary>
        /// <param name="objects">A raw block of data, which has not been grouped into objects</param>
        static IEnumerable<ObjectGraph> CreateObject(ObjectPropertyGraph propertyGraph, IEnumerable<object[]> rows)
        {
            // group the data into individual objects, where an object has multiple rows (for sub properties which are enumerable)
            var objectsData = rows.GroupBy(r => 
                propertyGraph.RowIdColumnNumbers.Select(i => r[i]).ToArray(), 
                ArrayComparer<object>.Instance);

            return CreateObject(propertyGraph, objectsData);
        }

        /// <summary>
        /// Map a group of rows to an object property graph to an object graph with properties
        /// </summary>
        /// <param name="objects">An enumerable of objects. Each object can span multiple rows (corresponding to sub properties which are enumerable)</param>
        static IEnumerable<ObjectGraph> CreateObject(ObjectPropertyGraph propertyGraph, IEnumerable<IEnumerable<object[]>> objects)
        {
            foreach (var objectData in objects)
            {
                yield return new ObjectGraph
                {
                    // simple prop values can be found by their column index
                    SimpleProps = propertyGraph.SimpleProps
                        .Select(GetSimpleProp)
                        .Enumerate(),
                    // complex prop values are built recursively
                    ComplexProps = propertyGraph.ComplexProps
                        .Select(p => (p.name, CreateObject(p.value, objectData).Enumerate()))
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
