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
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">The executor to execute sql</param>
        /// <param name="sqlBuilder">The builder with all properties populated</param>
        /// <param name="parameters">Any constant parameters in the statement</param>
        /// <param name="primaryTableName">The name of the table in the select statement</param>
        public static async Task<IEnumerable<TResult>> ExecuteAsync<TResult>(this IExecutor executor, ISqlStatement sqlBuilder, IEnumerable<object> parameters, string primaryTableName) 
        {
            var sql = ToSql(sqlBuilder);
            var selectColumns = sqlBuilder.SelectColumns.ToArray();
            
            // remove "no root" object alias if necessary
            primaryTableName = primaryTableName == SqlStatementConstants.RootObjectAlias ?
                null :
                primaryTableName;

            // get index of the column for row id of the primary table
            var primaryRowIdName = primaryTableName == null ?
                SqlStatementConstants.RowIdName :
                $"{primaryTableName}.{SqlStatementConstants.RowIdName}";

            // build row ID map
            var rowIdColumns = sqlBuilder.RowIdMap.ToList();
            var rowIdMap = selectColumns
                .Select(c => 
                {
                    var op = rowIdColumns
                        .Where(rid => rid.columnName == c)
                        .Select(rid => rid.rowIdColumnName)
                        .FirstOrDefault() ??
                        throw new InvalidOperationException($"Cannot find row id for column {c}");

                    var index = selectColumns.IndexOf(op);
                    if (index == -1) throw new InvalidOperationException($"Cannot find row id for column {c}");

                    return sqlBuilder
                        .GetDependantRowIds(index)
                        .Append(index)
                        .ToArray();
                })
                .ToArray();

            // get primary row Id
            var primaryRowId = selectColumns.IndexOf(primaryRowIdName);
            if (primaryRowId == -1)
                throw new InvalidOperationException($"Could not find row id column for table {primaryTableName}");

            // execute and get all rows
            var reader = await executor.ExecuteDebugAsync(sql, parameters, selectColumns);
            var results = await reader.GetRowsAsync();

            // TODO: compile and cache ObjectProperty graph, and use as first arg
            return results.Parse<TResult>(selectColumns, rowIdMap, primaryRowId);
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