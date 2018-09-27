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
            // map each mapped property to a chain of row id column numbers
            var properties = sqlBuilder
                .RowIdsForMappedProperties
                .Select(GetProperty);

            // map each column to a chain of row id column numbers
            var rowIdColumns = sqlBuilder
                .RowIdMap
                .Select(BuildRIDC)
                .OrderBy(GetIndex)
                .Select(RemoveIndex);
                
            return OPG.Build(objectType, properties, rowIdColumns, queryParseType);

            (string name, int[] rowIdColumnMap) GetProperty((string rowIdColumnName, string resultClassProperty) propertyRowIds)
            {
                var ridI = sqlBuilder.SelectColumns.IndexOf(propertyRowIds.rowIdColumnName);
                if (ridI == -1)
                    throw new InvalidOperationException($"Could not find index for column: {propertyRowIds.rowIdColumnName}");

                return (
                    propertyRowIds.resultClassProperty, 
                    sqlBuilder.GetDependantRowIdChain(ridI).ToArray()
                );
            }

            (int index, string name, int[] indexes) BuildRIDC((string columnName, string rowIdColumnName) m, int i)
            {
                // TODO: this should be part of builder?

                var ridI = sqlBuilder.SelectColumns.IndexOf(m.rowIdColumnName);
                if (ridI == -1)
                    throw new InvalidOperationException($"Could not find index for column: {m.rowIdColumnName}");

                var colIndex = sqlBuilder.SelectColumns.IndexOf(m.columnName);
                if (colIndex == -1)
                    throw new InvalidOperationException($"Could not find index for column: {m.columnName}");

                var indexes = sqlBuilder.GetDependantRowIdChain(ridI);

                
                Console.WriteLine($"{colIndex} {m.columnName} [{indexes.JoinString(",")}]");
                return (colIndex, m.columnName, indexes.ToArray());
            }

            int GetIndex((int index, string name, int[] indexes) x) => x.index;

            (string name, int[] indexes) RemoveIndex((int index, string name, int[] indexes) x) => (x.name, x.indexes);
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