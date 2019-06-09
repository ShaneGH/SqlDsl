using System;
using System.Collections.Generic;
using SqlDsl.SqlBuilders;
using System.Threading.Tasks;
using System.Linq;
using SqlDsl.Utils;
using SqlDsl.DataParser;
using SqlDsl.SqlBuilders.SqlStatementParts;
using System.Reflection;
using SqlDsl.Utils.EqualityComparers;

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
            QueryParseType queryParseType,
            bool requiresSimpleValueUnwrap)
        {
            var sql = ToSql(sqlBuilder);

            var selectColumns = statement.SelectColumns.Select(Alias).ToArray();
            var resultType = requiresSimpleValueUnwrap 
                ? ReflectionUtils.CreatePropMapValue(typeof(TResult)) 
                : typeof(TResult);
            var propertyGraph = statement.BuildObjetPropertyGraph(resultType, queryParseType);

            return new CompiledQuery<TArgs, TResult>(sql, parameters.ToArray(), selectColumns, propertyGraph, sqlSyntax, requiresSimpleValueUnwrap);
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
            var columnWithPrimaryKeys = sqlBuilder
                .SelectColumns
                .Select(GetMappedColumn);
                
            return ObjectPropertyGraphBuilder.Build(objectType, mappedTableProperties, columnWithPrimaryKeys, queryParseType);

            (string name, int[] primaryKeyColumnMap) GetMappedTable((string columnGroupPrefix, ISelectColumn rowNumberColumn) map) => (
                map.columnGroupPrefix,
                sqlBuilder.GetRowNumberColumnIndexes(map.rowNumberColumn.Alias, map.rowNumberColumn.IsRowNumberForTable).ToArray());

            (string name, int[] primaryKeyColumnMap, Type dataCellType, ConstructorInfo[] isConstructorArg) GetMappedColumn(ISelectColumn column) => (
                column.Alias,
                sqlBuilder
                    .GetRowNumberColumnIndexes(
                        column.Alias, 
                        TablePrecedenceOrderer.GetSingleMappingContext(column))
                    .ToArray(),
                column.DataType,
                column.ArgConstructors);
        }

        /// <summary>
        /// Get a sql statement and corresponding sql paramaters from the builder
        /// </summary>
        /// <param name="builder">The sql builder to use in order to render sql</param>
        public static QueryParts ToSql(this ISqlString builder)
        {
            var builderResult = builder.ToSqlString();
            var sql = builderResult.Sql;
            var queryTeardownSql = builderResult.QueryTeardownSql;

            if (builderResult.TeardownSqlCanBeInlined)
            {
                sql += $";\n\n{queryTeardownSql}";
                queryTeardownSql = null;
            }

            return new QueryParts(
                $"{builderResult.QuerySetupSql}\n\n{sql}",
                queryTeardownSql);
        }  
    }
}