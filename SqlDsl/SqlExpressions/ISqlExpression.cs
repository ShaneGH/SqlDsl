using System;
using System.Collections.Generic;
using System.Linq;
using SqlDsl.Mapper;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.SqlExpressions
{
    interface ISqlExpression<TElement>
    {
        bool HasOneItemOnly { get; }
        TElement First  { get; }
        AggregationType AggregationType { get; }
        IEnumerable<TElement> GetEnumerable();
        IEnumerable<(bool isAggregated, TElement element)> GetAggregatedEnumerable();
        ISqlExpression<T> MapParam<T>(Func<TElement, T> map);
        ISqlExpression<TElement> Combine(ISqlExpression<TElement> x, BinarySqlOperator combiner);
        string BuildFromString(BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias = null);
    }

    enum AggregationType
    {
        IsAggregated,
        NotAggregated,
        ContainsAggregatedPart
    }

    static class ISqlExpressionUtils
    {
        public static Func<StringBasedElement, (string param, string aggregatedToTable)> AddRoot(BuildMapState state)
        {
            return Execute;

            (string, string) Execute(StringBasedElement x)
            {
                // I am not 100% sure about the "root == state.QueryObject" part
                if (x.ParamRoot == null || x.ParamRoot == state.QueryObject) return (x.Param, x.AggregatedToTable);

                var propertyRoot = state.ParameterRepresentsProperty
                    .Where(p => p.parameter == x.ParamRoot)
                    .Select(p => p.property.JoinString("."))
                    .FirstOrDefault();

                if (propertyRoot == null)
                    throw new InvalidOperationException($"Could not find alias for mapping parameter \"{x.ParamRoot}\". {x.Param}");

                if (!string.IsNullOrEmpty(x.Param))
                    propertyRoot += ".";

                return ($"{propertyRoot}{x.Param}", x.AggregatedToTable);
            }
        }

        public static (string param, string aggregatedToTable) AddRoot(this StringBasedElement value, BuildMapState state) => AddRoot(state)(value);

        public static ISqlExpression<SelectColumnBasedElement> Convert(this ISqlExpression<StringBasedElement> acc, BuildMapState state)
        {
            IQueryTable primaryTable = null;
            return acc.MapParam(Map);

            SelectColumnBasedElement Map(StringBasedElement el)
            {
                var (fullName, overrideTable) = el.AddRoot(state);
                if (fullName.StartsWith("@"))
                    return MapParameter(el, fullName, overrideTable);

                var tableName = GetTableName(fullName);
                var col = state.WrappedSqlStatement.SelectColumns[fullName];
                var tab = state.WrappedSqlStatement.Tables[overrideTable ?? tableName];
                var rid = tab.RowNumberColumn;

                return new SelectColumnBasedElement(col, rid);
            }

            SelectColumnBasedElement MapParameter(StringBasedElement el, string fullName, string overrideTable)
            {
                IQueryTable aggregatedToTable;
                if (overrideTable != null)
                {
                    aggregatedToTable = state.WrappedSqlStatement.Tables[overrideTable];
                }
                else
                {
                    if (primaryTable == null)
                        primaryTable = state.WrappedSqlStatement.Tables[state.PrimarySelectTableAlias];

                    aggregatedToTable = primaryTable;
                }

                return new SelectColumnBasedElement(fullName, aggregatedToTable.RowNumberColumn);
            }
        }

        static string GetTableName(string fullName)
        {
            var i = fullName.LastIndexOf('.');
            return i == -1 ?
                SqlStatementConstants.RootObjectAlias : 
                fullName.Substring(0, i);
        }

        public static ISqlExpression<StringBasedElement> MapParamName(this ISqlExpression<StringBasedElement> acc, Func<string, string> map)
        {
            return acc.MapParam(_Map);
            StringBasedElement _Map(StringBasedElement el) => new StringBasedElement(el.ParamRoot, map(el.Param), el.AggregatedToTable);
        }

        public static string Combine(ISqlSyntax sqlFragmentBuilder, string l, string r, BinarySqlOperator combine)
        {
            switch (combine)
            {
                case BinarySqlOperator.And:
                    return sqlFragmentBuilder.BuildAndCondition(l, r);
                    
                case BinarySqlOperator.Or:
                    return sqlFragmentBuilder.BuildOrCondition(l, r);
                    
                case BinarySqlOperator.Add:
                    return sqlFragmentBuilder.BuildAddCondition(l, r);

                case BinarySqlOperator.Subtract:
                    return sqlFragmentBuilder.BuildSubtractCondition(l, r);
                    
                case BinarySqlOperator.Multiply:
                    return sqlFragmentBuilder.BuildMultiplyCondition(l, r);
                    
                case BinarySqlOperator.Divide:
                    return sqlFragmentBuilder.BuildDivideCondition(l, r);
                    
                case BinarySqlOperator.In:
                    return sqlFragmentBuilder.BuildInCondition(l, r);
                    
                case BinarySqlOperator.Comma:
                    return sqlFragmentBuilder.BuildCommaCondition(l, r);
                    
                case BinarySqlOperator.Equal:
                    return sqlFragmentBuilder.BuildEqualityCondition(l, r);
                    
                case BinarySqlOperator.NotEqual:
                    return sqlFragmentBuilder.BuildNonEqualityCondition(l, r);
                    
                case BinarySqlOperator.GreaterThan:
                    return sqlFragmentBuilder.BuildGreaterThanCondition(l, r);
                    
                case BinarySqlOperator.GreaterThanOrEqual:
                    return sqlFragmentBuilder.BuildGreaterThanEqualToCondition(l, r);
                    
                case BinarySqlOperator.LessThan:
                    return sqlFragmentBuilder.BuildLessThanCondition(l, r);
                    
                case BinarySqlOperator.LessThanOrEqual:
                    return sqlFragmentBuilder.BuildLessThanEqualToCondition(l, r);

                default:
                    throw new InvalidOperationException($"Cannot build sql expression for expression type {combine}.");
            }
        }
    }
}