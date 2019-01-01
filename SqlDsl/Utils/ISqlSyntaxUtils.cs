using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.Mapper;

namespace SqlDsl.SqlBuilders
{
    /// <summary>
    /// Extension methods for ISqlSyntax
    /// </summary>
    public static class ISqlSyntaxUtils
    {
        /// <summary>
        /// Concat 2 query parts in a defined way (e.g. l + r)
        /// </summary>
        public static string Concat(this ISqlSyntax builder, string l, string r, BinarySqlOperator concatType)
        {
            switch (concatType)
            {
                case BinarySqlOperator.And:
                    return builder.BuildAndCondition(l, r);
                case BinarySqlOperator.Or:
                    return builder.BuildOrCondition(l, r);
                case BinarySqlOperator.Add:
                    return builder.BuildAddCondition(l, r);
                case BinarySqlOperator.Subtract:
                    return builder.BuildSubtractCondition(l, r);
                case BinarySqlOperator.Multiply:
                    return builder.BuildMultiplyCondition(l, r);
                case BinarySqlOperator.Divide:
                    return builder.BuildDivideCondition(l, r);
                case BinarySqlOperator.In:
                    return builder.BuildInCondition(l, r);
                case BinarySqlOperator.Comma:
                    return builder.BuildCommaCondition(l, r);
                case BinarySqlOperator.Equal:
                    return builder.BuildEqualityCondition(l, r);
                case BinarySqlOperator.NotEqual:
                    return builder.BuildNonEqualityCondition(l, r);
                case BinarySqlOperator.GreaterThan:
                    return builder.BuildGreaterThanCondition(l, r);
                case BinarySqlOperator.GreaterThanOrEqual:
                    return builder.BuildGreaterThanEqualToCondition(l, r);
                case BinarySqlOperator.LessThan:
                    return builder.BuildLessThanCondition(l, r);
                case BinarySqlOperator.LessThanOrEqual:
                    return builder.BuildLessThanEqualToCondition(l, r);
                default:
                    throw new NotSupportedException($"Cannot combine query parts using concatType: {concatType}.");
            }
        }

        /// <summary>
        /// Build the string for a SELECT column
        /// </summary>
        public static string BuildSelectColumn(this ISqlSyntax builder, string tableName, string columnName)
        {
            columnName = (columnName ?? "").StartsWith("@") ? columnName : builder.WrapColumn(columnName);

            return tableName == null ? 
                $"{columnName}" : 
                $"{builder.WrapTable(tableName)}.{columnName}";
        }

        /// <summary>
        /// Add an alias to a SELECT column
        /// </summary>
        public static string AddAliasColumn(this ISqlSyntax builder, string sqlCode, string alias)
        {
            if (string.IsNullOrEmpty(alias) ||
                alias.StartsWith($"{SqlStatementConstants.RootObjectAlias}."))
                return sqlCode;

            return builder.BuildAlias(sqlCode, builder.WrapAlias(alias));
        }
    }
}
