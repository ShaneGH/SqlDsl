using System;
using System.Collections.Generic;
using SqlDsl.SqlBuilders;
using System.Threading.Tasks;
using System.Linq;
using SqlDsl.Utils;
using SqlDsl.DataParser;
using SqlDsl.SqlBuilders.SqlStatementParts;

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
        public static CompiledQuery<TArgs, TResult> Compile<TArgs, TResult> (this SqlStatementBuilder sqlBuilder, IEnumerable<object> parameters, QueryParseType queryParseType)
        {
            var sql = ToSql(sqlBuilder);
            var statement = new SqlStatement(sqlBuilder);

            var selectColumns = statement.SelectColumns.Select(Alias).ToArray();
            var propertyGraph = statement.BuildObjetPropertyGraph(typeof(TResult), queryParseType);

            return new CompiledQuery<TArgs, TResult>(sql, parameters, selectColumns, propertyGraph);
        }

        /// <summary>
        /// Compile a sqlBuilder into a query which can be executed multiple times, The query in this case should return one simple value
        /// </summary>
        /// <param name="sqlBuilder">The builder with the property populated</param>
        /// <param name="parameters">Any constant parameters in the statement</param>
        /// <param name="property">The name of the singe property</param>
        public static CompiledQuery<TArgs, TResult> CompileSimple<TArgs, TResult>(this SqlStatementBuilder sqlBuilder, IEnumerable<object> parameters, string property)
        {
            var statement = new SqlStatement(sqlBuilder);
            var i = statement.IndexOfColumnAlias(property);
            if (i == -1)
                throw new InvalidOperationException($"Could not find column {property} in wrapped statement.");

            var col = statement.SelectColumns[i];
            var graph = new RootObjectPropertyGraph(typeof(TResult), i, col.RowNumberColumnIndex, ReflectionUtils.GetIEnumerableType(col.DataType) != null);
            return new CompiledQuery<TArgs, TResult>(sqlBuilder.ToSql(), parameters, statement.SelectColumns.Select(Alias).ToArray(), graph);
        }

        static string Alias(ISelectColumn c) => c.Alias;
        
        /// <summary>
        /// Build an object property graph from a sql builder
        /// </summary>
        /// <param name="sqlBuilder">The builder with all properties populated</param>
        /// <param name="queryParseType">Define the way results are to be parsed</param>
        public static RootObjectPropertyGraph BuildObjetPropertyGraph(this ISqlStatement sqlBuilder, Type objectType, QueryParseType queryParseType) 
        {
            // row id's for each mapped table
            var mappedTableProperties = (sqlBuilder.MappingProperties
                ?.ColumnGroupRowNumberColumIndex
                .Select(GetMappedTable))
                .OrEmpty();

            // map each column to a chain of row id column numbers
            var rowIdColumns = sqlBuilder
                .SelectColumns
                .Select(GetMappedColumn);
                
            return ObjectPropertyGraphBuilder.Build(objectType, mappedTableProperties, rowIdColumns, queryParseType);

            (string name, int[] rowIdColumnMap) GetMappedTable((string columnGroupPrefix, int rowNumberColumnIndex) map) => (
                map.columnGroupPrefix,
                sqlBuilder.GetRowNumberColumnIndexes(map.rowNumberColumnIndex).ToArray());

            (string name, int[] rowIdColumnMap, Type dataCellType) GetMappedColumn(ISelectColumn column) => (
                column.Alias,
                sqlBuilder.GetRowNumberColumnIndexes(column.Alias).ToArray(),
                column.DataType);
        }

        /// <summary>
        /// Get a sql statement and corresponding sql paramaters from the builder
        /// </summary>
        /// <param name="builder">The sql builder to use in order to render sql</param>
        public static string ToSql(this ISqlBuilder builder)
        {
            var sql = builder.ToSqlString();
            return $"{sql.querySetupSql}\n\n{sql.querySql}";
        }  
    }
}