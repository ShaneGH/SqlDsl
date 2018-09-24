using System;
using System.Collections.Generic;
using SqlDsl.SqlBuilders;
using System.Threading.Tasks;
using System.Linq;
using SqlDsl.Utils;
using SqlDsl.DataParser;

namespace SqlDsl.Query
{
    public static class QueryExecutionUtils
    {
        /// <summary>
        /// Compile a sqlBuilder into a query which can be executed multiple times
        /// </summary>
        /// <param name="sqlBuilder">The builder with all properties populated</param>
        /// <param name="parameters">Any constant parameters in the statement</param>
        /// <param name="queryParseType">Define the way results are to be parsed</param>
        public static CompiledQuery<TResult> Compile<TResult>(this ISqlStatement sqlBuilder, IEnumerable<object> parameters, QueryParseType queryParseType) 
        {
            var sql = ToSql(sqlBuilder);
            var selectColumns = sqlBuilder.SelectColumns.ToArray();
            var propertyGraph = sqlBuilder.BuildObjetPropertyGraph(typeof(TResult), queryParseType);

            return new CompiledQuery<TResult>(sql, parameters, selectColumns, propertyGraph);
        }
        
        /// <summary>
        /// Build an object property graph from a sql builder
        /// </summary>
        /// <param name="sqlBuilder">The builder with all properties populated</param>
        /// <param name="queryParseType">Define the way results are to be parsed</param>
        public static RootObjectPropertyGraph BuildObjetPropertyGraph(this ISqlStatement sqlBuilder, Type objectType, QueryParseType queryParseType) 
        {
            // map each column to a chain of row id column numbers
            var rowIdColumns = sqlBuilder
                .RowIdMap
                .Select((m, i) => 
                {
                    // TODO: this should be part of builder?

                    var ridI = sqlBuilder.SelectColumns.IndexOf(m.rowIdColumnName);
                    if (ridI == -1)
                        throw new InvalidOperationException($"Could not find index for column: {m.rowIdColumnName}");

                    var colIndex = sqlBuilder.SelectColumns.IndexOf(m.columnName);
                    if (colIndex == -1)
                        throw new InvalidOperationException($"Could not find index for column: {m.columnName}");

                    var indexes = sqlBuilder.GetDependantRowIdChain(ridI);
                    return (i: colIndex, n: m.columnName, ixs: indexes.ToArray());
                })
                .OrderBy(x => x.i)
                .Select(x => (x.n, x.ixs));
                
            return OPG.Build(objectType, rowIdColumns, queryParseType);




            // var rowIdMap = selectColumns
            //     .Select(c => 
            //     {
            //         var op = rowIdColumns
            //             .Where(rid => rid.columnName == c)
            //             .Select(rid => rid.rowIdColumnName)
            //             .FirstOrDefault() ??
            //             throw new InvalidOperationException($"Cannot find row id for column \"{c}\".");

            //         var index = selectColumns.IndexOf(op);
            //         if (index == -1) throw new InvalidOperationException($"Cannot find row id for column \"{c}\".");

            //         return sqlBuilder
            //             .GetDependantRowIds(index)
            //             .Append(index)
            //             .ToArray();
            //     })
            //     .ToArray();

            // // convert from mapped properties to property -> rowId pointer
            // var map = sqlBuilder.RowIdsForMappedProperties
            //     .Select(ridm => (
            //         prop: ridm.resultClassProperty, 
            //         selectColumns
            //             .IndexOf(ridm.rowIdColumnName)
            //             .ToEnumerableStruct())
            //     )
            //     .Enumerate();

            // // test previous step for errors
            // foreach (var mapped in map)
            //     if (mapped.Item2.First() == -1)
            //         throw new InvalidOperationException("Could not find row id column for " + mapped.prop);

            // var propertyGraph = OPG.Build(objectType,  new RootObjectPropertyGraph(rowIdMap, selectColumns, objectType, map);
            // return (propertyGraph, rowIdMap);
        }

        /// <summary>
        /// Get a sql statement and corresponding sql paramaters from the builder
        /// </summary>
        /// <param name="builder">The sql builder to use in order to render sql</param>
        public static string ToSql(this ISqlStatement builder)
        {
            var sql = builder.ToSqlString();
            return $"{sql.querySetupSql}\n\n{sql.querySql}";
        }  
    }
}