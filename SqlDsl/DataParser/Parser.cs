using SqlDsl.ObjectBuilders;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using System;
using System.Collections;
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
        internal static IEnumerable<TResult> Parse<TResult>(this IEnumerable<object[]> rows, RootObjectPropertyGraph propertyGraph, ILogger logger)
        {
            return propertyGraph.IsSimpleValue ?
                ParseSimple<TResult>(rows, propertyGraph, logger).Enumerate() :
                ParseComplex<TResult>(rows, propertyGraph, logger).Enumerate();
        }

        /// <summary>
        /// Parse the results of a sql query which returns one column
        /// </summary>
        /// <param name="rows">The query results</param>
        /// <param name="propertyGraph">The query columns mapped to an object graph</param>
        static IEnumerable<TResult> ParseSimple<TResult>(this IEnumerable<object[]> rows, RootObjectPropertyGraph propertyGraph, ILogger logger)
        {
            // group the data into individual objects, where an object has multiple rows (for sub properties which are enumerable)
            var values = rows
                // simple mapped properties are always grouped around the primary select
                .GroupBy(r => r[0])
                .Select(r => r.First()[propertyGraph.SimpleValueColumnIndex]);

            var convertor = TypeConvertors.GetConvertor<TResult>(propertyGraph.SimplePropertyCellTypeIsEnumerable);
            foreach (var value in values)
                yield return convertor(value, logger);
        }

        /// <summary>
        /// Parse the results of a sql query which returns a comlex object
        /// </summary>
        /// <param name="rows">The query results</param>
        /// <param name="propertyGraph">The query columns mapped to an object graph</param>
        static IEnumerable<TResult> ParseComplex<TResult>(this IEnumerable<object[]> rows, RootObjectPropertyGraph propertyGraph, ILogger logger)
        {
            foreach (var obj in CreateObject(propertyGraph, rows))
                yield return (TResult)Builders.Build(typeof(TResult), obj, logger);
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

                (string name, IEnumerable<object> value, Action<object, IEnumerable, ILogger> customSetter, bool isEnumerableDataCell) GetSimpleProp((int index, string name, IEnumerable<int> rowNumberColumnIds, Type resultPropertyType, Type dataCellType) p)
                {
                    // run a "Distinct" on the rowNumbers
                    var dataRowsForProp = objectData
                        .GroupBy(d => propertyGraph.GetUniqueIdForSimpleProp(d, p.rowNumberColumnIds))
                        .Select(Enumerable.First);

                    var data = dataRowsForProp
                        .Select(o => o[p.index])
                        .ToArray();

                    var cellEnumType = p.dataCellType == null ?
                        null :
                        ReflectionUtils.GetIEnumerableType(p.dataCellType);

                    // regular parsers will not work if the cell contains an array
                    // e.g. byte[]
                    var customSetter = cellEnumType == null ?
                        null :
                        Objects.GetEnumerableSetter(propertyGraph.ObjectType, p.name, p.resultPropertyType);

                    return (p.name, data, customSetter, cellEnumType != null);
                }
            }
        }
    }
}
