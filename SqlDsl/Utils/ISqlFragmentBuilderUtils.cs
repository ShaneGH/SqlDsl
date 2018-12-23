using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.Mapper;

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
        public static string Concat(this ISqlFragmentBuilder builder, string l, string r, CombinationType concatType)
        {
            switch (concatType)
            {
                case CombinationType.And:
                    return builder.BuildAndCondition(l, r);
                case CombinationType.Or:
                    return builder.BuildOrCondition(l, r);
                case CombinationType.Add:
                    return builder.BuildAddCondition(l, r);
                case CombinationType.Subtract:
                    return builder.BuildSubtractCondition(l, r);
                case CombinationType.Multiply:
                    return builder.BuildMultiplyCondition(l, r);
                case CombinationType.Divide:
                    return builder.BuildDivideCondition(l, r);
                case CombinationType.In:
                    return builder.BuildInCondition(l, r);
                case CombinationType.Comma:
                    return builder.BuildCommaCondition(l, r);
                case CombinationType.Equal:
                    return builder.BuildEqualityCondition(l, r);
                case CombinationType.NotEqual:
                    return builder.BuildNonEqualityCondition(l, r);
                case CombinationType.GreaterThan:
                    return builder.BuildGreaterThanCondition(l, r);
                case CombinationType.GreaterThanOrEqual:
                    return builder.BuildGreaterThanEqualToCondition(l, r);
                case CombinationType.LessThan:
                    return builder.BuildLessThanCondition(l, r);
                case CombinationType.LessThanOrEqual:
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
