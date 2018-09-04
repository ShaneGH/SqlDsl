using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace SqlDsl.Query
{
    public partial class QueryBuilder<TSqlBuilder, TResult>
        where TSqlBuilder: ISqlFragmentBuilder, new()
    {
        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        public async Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor) 
        {
            if (PrimaryTableMember == null)
                throw new InvalidOperationException("You must set the FROM table before calling ToSql");

            var sqlBuilder = ToSqlBuilder(null);
            var sql = ToSql(sqlBuilder.builder);
            var selectColumns = sqlBuilder.builder.SelectColumns.ToArray();

            var reader = await executor.ExecuteDebugAsync(sql, sqlBuilder.paramaters, selectColumns);
            var results = await reader.GetRowsAsync();

            var primaryTableName = PrimaryTableMember.Value.name == SqlStatementConstants.RootObjectAlias ?
                null :
                PrimaryTableMember.Value.name;

            var rowIdColumns = sqlBuilder.builder.RowIdMap.ToList();
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

                    return index;
                })
                .ToArray();

            // TODO: compile and cache ObjectProperty graph, and use as first arg
            return results.Parse<TResult>(selectColumns, rowIdMap, primaryTableName);
        }
    }
}
