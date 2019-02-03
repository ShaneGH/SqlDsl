using SqlDsl.Mapper;
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
        internal static IEnumerable<TResult> Parse<TResult>(this IEnumerable<object[]> rows, RootObjectPropertyGraph propertyGraph, ILogger logger, bool requiresSimpleValueUnwrap)
        {
            return requiresSimpleValueUnwrap ?
                ParseSimple<TResult>(rows, propertyGraph, logger) :
                ParseComplex<TResult>(rows, propertyGraph, logger, InvalidPropValueCache.Instance);
        }

        /// <summary>
        /// Parse the results of a sql query which returns one column
        /// </summary>
        /// <param name="rows">The query results</param>
        /// <param name="propertyGraph">The query columns mapped to an object graph</param>
        static IEnumerable<TResult> ParseSimple<TResult>(this IEnumerable<object[]> rows, RootObjectPropertyGraph propertyGraph, ILogger logger)
        {
            var propMapBuilder = new PropMapValueCache<TResult>(logger);
            foreach (var value in rows.ParseComplex<PropMapValue<TResult>>(propertyGraph, logger, propMapBuilder))
            {
                yield return value.Value;
                value.Dispose();
            }
        }

        /// <summary>
        /// Parse the results of a sql query which returns a comlex object
        /// </summary>
        /// <param name="rows">The query results</param>
        /// <param name="propertyGraph">The query columns mapped to an object graph</param>
        static IEnumerable<TResult> ParseComplex<TResult>(this IEnumerable<object[]> rows, RootObjectPropertyGraph propertyGraph, ILogger logger, IPropMapValueCache propMapBuilder)
        {   
            var objectGraphCache = new ObjectGraphCache(logger);
            var builder = Builders.GetBuilder<TResult>();
            foreach (var obj in CreateObject(propertyGraph, objectGraphCache, rows, logger))
            {
                var result = builder.Build(obj, propMapBuilder, logger);
                obj.Dispose();
                yield return result;
            }
        }

        /// <summary>
        /// Map a group of rows to an object property graph to an object graph with properties
        /// </summary>
        /// <param name="objects">A raw block of data, which has not been grouped into objects</param>
        static IEnumerable<ObjectGraph> CreateObject(ObjectPropertyGraph propertyGraph, ObjectGraphCache objectGraphCache, IEnumerable<object[]> rows, ILogger logger)
        {
            var objectsData = propertyGraph.GroupAndFilterData(rows);
            return CreateObject(propertyGraph, objectGraphCache, objectsData, logger);
        }

        /// <summary>
        /// Map a group of rows to an object property graph to an object graph with properties
        /// </summary>
        /// <param name="objects">An enumerable of objects. Each object can span multiple rows (corresponding to sub properties which are enumerable)</param>
        static IEnumerable<ObjectGraph> CreateObject(ObjectPropertyGraph propertyGraph, ObjectGraphCache objectGraphCache, IEnumerable<IEnumerable<object[]>> objects, ILogger logger)
        {
            foreach (var objectData in objects)
            {
                var graph = objectGraphCache.ReleseOrCreateItem();
                graph.Init(propertyGraph, objectData);
                yield return graph;
            }
        }

        class InvalidPropValueCache : IPropMapValueCache
        {
            public static readonly IPropMapValueCache Instance = new InvalidPropValueCache();

            private InvalidPropValueCache()
            {
            }

            public object ReleaseOrCreateItem()
            {
                throw new InvalidOperationException($"{typeof(PropMapValueCache<>)} is for internal use only.");
            }
        }
    }
}
