using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    /// <summary>
    /// Extension methods for Reflection
    /// </summary>
    public static class ISqlFragmentBuilderUtils
    {
        /// <summary>
        /// Build the string for a SELECT column
        /// </summary>
        public static string BuildSelectColumn(this ISqlFragmentBuilder builder, string tableName, string columnName)
        {
            columnName = (columnName ?? "").StartsWith("@") ? columnName : builder.WrapColumn(columnName);

            return tableName == null ? 
                $"{columnName}" : 
                $"{builder.WrapTable(tableName)}.{columnName}";
        }

        /// <summary>
        /// Add an alias to a SELECT column
        /// </summary>
        public static string AddAliasColumn(this ISqlFragmentBuilder builder, string sqlCode, string alias)
        {
            if (string.IsNullOrEmpty(alias) ||
                alias.StartsWith($"{SqlStatementConstants.RootObjectAlias}."))
                return sqlCode;

            // TODO: AS should go in SqlBuilder
            return $"{sqlCode} AS {builder.WrapAlias(alias)}";
        }
    }
}
