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
        /// Concat 2 query parts in a defined way (e.g. l + r)
        /// </summary>
        public static string Concat(this ISqlFragmentBuilder builder, string l, string r, ExpressionType concatType)
        {
            switch (concatType)
            {
                case ExpressionType.Add:
                    return builder.BuildAddCondition(l, r);
                case ExpressionType.Subtract:
                    return builder.BuildSubtractCondition(l, r);
                case ExpressionType.Multiply:
                    return builder.BuildMultiplyCondition(l, r);
                case ExpressionType.Divide:
                    return builder.BuildDivideCondition(l, r);
                case ExpressionType.OnesComplement:
                    return builder.BuildInCondition(l, r);
                case ExpressionType.Modulo:
                    return builder.BuildCommaCondition(l, r);
                case ExpressionType.Equal:
                    return builder.BuildEqualityCondition(l, r);
                case ExpressionType.NotEqual:
                    return builder.BuildNonEqualityCondition(l, r);
                case ExpressionType.GreaterThan:
                    return builder.BuildGreaterThanCondition(l, r);
                case ExpressionType.GreaterThanOrEqual:
                    return builder.BuildGreaterThanEqualToCondition(l, r);
                case ExpressionType.LessThan:
                    return builder.BuildLessThanCondition(l, r);
                case ExpressionType.LessThanOrEqual:
                    return builder.BuildLessThanEqualToCondition(l, r);
                default:
                    throw new NotSupportedException($"Cannot combine query parts using concatType: {concatType}.");
            }
        }

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

            return builder.BuildAlias(sqlCode, builder.WrapAlias(alias));
        }
    }
}
