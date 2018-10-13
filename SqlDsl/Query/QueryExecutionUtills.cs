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
        public static CompiledQuery<TArgs, TResult> Compile<TArgs, TResult> (this SqlBuilderItems sqlBuilder, IEnumerable<object> parameters, QueryParseType queryParseType)
        {
            switch (sqlBuilder.QueryType)
            {
                case QueryType.Complex:
                    return Compile<TArgs, TResult>(sqlBuilder.Builder, sqlBuilder.Statement, parameters, queryParseType);
                case QueryType.Simple:
                    return Compile<TArgs, TResult>(
                        sqlBuilder.Builder, 
                        sqlBuilder.SimpleValueColumnIndex, 
                        sqlBuilder.SimpleValueRowNumberColumnIndex, 
                        parameters, 
                        queryParseType);
                default:
                    throw new NotSupportedException(sqlBuilder.QueryType.ToString());
            }
        }

        /// <summary>
        /// Compile a simple sqlBuilder into a query which can be executed multiple times
        /// </summary>
        static CompiledQuery<TArgs, TResult> Compile<TArgs, TResult> (
            ISqlBuilder builder, 
            int simpleValueColumnIndex, 
            int simpleValueRowNumberColumnIndex, 
            IEnumerable<object> parameters, 
            QueryParseType queryParseType)
        {
            var sql = ToSql(builder);

            // TODO
            var selectColumns = new[]{ "Debug select columns are not available for this type of query" };
            var propertyGraph = new RootObjectPropertyGraph(simpleValueColumnIndex, simpleValueRowNumberColumnIndex);

            return new CompiledQuery<TArgs, TResult>(sql, parameters, selectColumns, propertyGraph);
        }

        /// <summary>
        /// Compile a complex sqlBuilder into a query which can be executed multiple times
        /// </summary>
        static CompiledQuery<TArgs, TResult> Compile<TArgs, TResult> (ISqlBuilder builder, ISqlStatement statement, IEnumerable<object> parameters, QueryParseType queryParseType)
        {
            var sql = ToSql(builder);

            var selectColumns = statement.SelectColumns.Select(Alias).ToArray();
            var propertyGraph = statement.BuildObjetPropertyGraph(typeof(TResult), queryParseType);

            return new CompiledQuery<TArgs, TResult>(sql, parameters, selectColumns, propertyGraph);

            string Alias(ISelectColumn c) => c.Alias;
        }
        
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

            (string name, int[] rowIdColumnMap) GetMappedColumn(ISelectColumn column) => (
                column.Alias,
                sqlBuilder.GetRowNumberColumnIndexes(column.Alias).ToArray());
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