using SqlDsl.ObjectBuilders;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using SqlDsl.Utils.EqualityComparers;
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
                ParseSimple<TResult>(rows, propertyGraph, logger) :
                ParseComplex<TResult>(rows, propertyGraph, logger);
        }

        /// <summary>
        /// Parse the results of a sql query which returns one column
        /// </summary>
        /// <param name="rows">The query results</param>
        /// <param name="propertyGraph">The query columns mapped to an object graph</param>
        static IEnumerable<TResult> ParseSimple<TResult>(this IEnumerable<object[]> rows, RootObjectPropertyGraph propertyGraph, ILogger logger)
        {
            // group the data into individual objects, where an object has multiple rows (for sub properties which are enumerable)
            var dbValuesPerRecord = rows
                // simple mapped properties are always grouped around the primary select
                .GroupBy(r => r[0])
                .Select(singleRecord => singleRecord
                    .GroupBy(r => r[propertyGraph.SimpleValuePrimaryKeyColumn])
                    .Select(Enumerable.First))
                .Select(rs => rs.Select(r => r[propertyGraph.SimpleValueColumnIndex]));

            var resultEnumCount = ReflectionUtils.CountEnumerables(typeof(TResult));
            var cellEnumType = ReflectionUtils.CountEnumerables(propertyGraph.SimplePropertyCellType);

            var getter = ValueGetters.GetValueGetter<TResult>(resultEnumCount > cellEnumType, cellEnumType > 0);
            foreach (var value in dbValuesPerRecord)
                yield return getter(value, logger);
        }

        /// <summary>
        /// Parse the results of a sql query which returns a comlex object
        /// </summary>
        /// <param name="rows">The query results</param>
        /// <param name="propertyGraph">The query columns mapped to an object graph</param>
        static IEnumerable<TResult> ParseComplex<TResult>(this IEnumerable<object[]> rows, RootObjectPropertyGraph propertyGraph, ILogger logger)
        {   
            var objectGraphCache = new ObjectGraphCache();
            var builder = Builders.GetBuilder<TResult>();
            foreach (var obj in CreateObject(propertyGraph, objectGraphCache, rows, logger))
            {
                var result = builder.Build(obj, logger);
                obj.Dispose();
                yield return result;
            }
        }

        /// <summary>
        /// Map a group of rows to an object property graph to an object graph with properties
        /// </summary>
        /// <param name="objects">A raw block of data, which has not been grouped into objects</param>
        static IEnumerable<ReusableObjectGraph> CreateObject(ObjectPropertyGraph propertyGraph, ObjectGraphCache objectGraphCache, IEnumerable<object[]> rows, ILogger logger)
        {
            var objectsData = propertyGraph.GroupAndFilterData(rows);
            return CreateObject(propertyGraph, objectGraphCache, objectsData, logger);
        }

        /// <summary>
        /// Map a group of rows to an object property graph to an object graph with properties
        /// </summary>
        /// <param name="objects">An enumerable of objects. Each object can span multiple rows (corresponding to sub properties which are enumerable)</param>
        static IEnumerable<ReusableObjectGraph> CreateObject(ObjectPropertyGraph propertyGraph, ObjectGraphCache objectGraphCache, IEnumerable<IEnumerable<object[]>> objects, ILogger logger)
        {
            foreach (var objectData in objects)
            {
                var graph = objectGraphCache.GetGraph(logger);
                graph.Init(propertyGraph, objectData);
                yield return graph;
            }
        }
    }
}
