using System;
using System.Collections.Generic;
using SqlDsl.SqlBuilders;
using System.Threading.Tasks;
using System.Linq;
using SqlDsl.Utils;
using SqlDsl.DataParser;
using SqlDsl.SqlBuilders.SqlStatementParts;
using System.Reflection;

namespace SqlDsl.Query
{
    static class QueryExecutionUtils
    {        
        /// <summary>
        /// Compile a sqlBuilder into a query which can be executed multiple times
        /// </summary>
        /// <param name="sqlBuilder">The builder with all properties populated</param>
        /// <param name="parameters">Any constant parameters in the statement</param>
        /// <param name="queryParseType">Define the way results are to be parsed</param>
        public static CompiledQuery<TArgs, TResult> Compile<TArgs, TResult> (
            this ISqlString sqlBuilder, 
            ISqlSelectStatement statement, 
            IEnumerable<object> parameters,
            ISqlSyntax sqlSyntax, 
            QueryParseType queryParseType)
        {
            var sql = ToSql(sqlBuilder);

            var selectColumns = statement.SelectColumns.Select(Alias).ToArray();
            var propertyGraph = statement.BuildObjetPropertyGraph(typeof(TResult), queryParseType);

            return new CompiledQuery<TArgs, TResult>(sql, parameters.ToArray(), selectColumns, propertyGraph, sqlSyntax);
        }

        /// <summary>
        /// Compile a sqlBuilder into a query which can be executed multiple times, The query in this case should return one simple value
        /// </summary>
        /// <param name="sqlBuilder">The builder with the property populated</param>
        /// <param name="parameters">Any constant parameters in the statement</param>
        /// <param name="property">The name of the singe property</param>
        public static CompiledQuery<TArgs, TResult> CompileSimple<TArgs, TResult>(
            this ISqlString sqlBuilder,
            ISqlSelectStatement statement, 
            IEnumerable<object> parameters, 
            ISqlSyntax sqlSyntax, 
            string property)
        {
            var selectColumn = statement.SelectColumns[property];
            var graph = new RootObjectPropertyGraph(
                typeof(TResult), 
                statement.SelectColumns.IndexOf(selectColumn),
                // if Table == null, the selectColumn is a parameter
                // in this case it is indexed by the first column (rid of primary table)
                selectColumn.Table == null ? 0 : statement.SelectColumns.IndexOf(selectColumn.Table.RowNumberColumn),
                selectColumn.DataType,
                ReflectionUtils.GetIEnumerableType(selectColumn.DataType) != null);

            return new CompiledQuery<TArgs, TResult>(sqlBuilder.ToSql(), parameters.ToArray(), statement.SelectColumns.Select(Alias).ToArray(), graph, sqlSyntax);
        }

        static string Alias(ISelectColumn c) => c.Alias;
        
        /// <summary>
        /// Build an object property graph from a sql builder
        /// </summary>
        /// <param name="sqlBuilder">The builder with all properties populated</param>
        /// <param name="queryParseType">Define the way results are to be parsed</param>
        public static RootObjectPropertyGraph BuildObjetPropertyGraph(this ISqlSelectStatement sqlBuilder, Type objectType, QueryParseType queryParseType) 
        {
            // row id's for each mapped table
            var mappedTableProperties = sqlBuilder.MappedPropertiesToRowNumbers
                .Select(GetMappedTable);

            // map each column to a chain of row id column numbers
            var rowIdColumns = sqlBuilder
                .SelectColumns
                .Select(GetMappedColumn);
                
            return ObjectPropertyGraphBuilder.Build(objectType, mappedTableProperties, rowIdColumns, queryParseType);

            (string name, int[] rowIdColumnMap) GetMappedTable((string columnGroupPrefix, ISelectColumn rowNumberColumn) map) => (
                map.columnGroupPrefix,
                sqlBuilder.GetRowNumberColumnIndexes(map.rowNumberColumn.Alias, false).ToArray());

            (string name, int[] rowIdColumnMap, Type dataCellType, ConstructorInfo[] isConstructorArg) GetMappedColumn(ISelectColumn column) => (
                column.Alias,
                sqlBuilder.GetRowNumberColumnIndexes(column.Alias, column.IsAggregated).ToArray(),
                column.DataType,
                column.ArgConstructors);
        }

        /// <summary>
        /// Get a sql statement and corresponding sql paramaters from the builder
        /// </summary>
        /// <param name="builder">The sql builder to use in order to render sql</param>
        public static QueryParts ToSql(this ISqlString builder)
        {
            var sql = builder.ToSqlString();
            return new QueryParts(
                $"{sql.querySetupSql}\n\n{sql.beforeWhereSql}",
                sql.whereSql,
                sql.afterWhereSql);
        }  
    }
}