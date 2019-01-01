using System;
using System.Collections.Generic;
using System.Linq;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    interface IAccumulator<TElement>
    {
        bool HasOneItemOnly { get; }
        TElement First  { get; }
        AggregationType AggregationType { get; }
        IEnumerable<TElement> GetEnumerable();
        IEnumerable<(bool isAggregated, TElement element)> GetAggregatedEnumerable();
        IAccumulator<T> MapParam<T>(Func<TElement, T> map);
        IAccumulator<TElement> Combine(IAccumulator<TElement> x, BinarySqlOperator combiner);
    }

    enum AggregationType
    {
        IsAggregated,
        NotAggregated,
        ContainsAggregatedPart
    }

    static class IAccumulatorUtils
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

        public static IAccumulator<SelectColumnBasedElement> Convert(this IAccumulator<StringBasedElement> acc, BuildMapState state)
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
                        primaryTable = state.WrappedSqlStatement.Tables[state.PrimarySelectTable];

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

        public static IAccumulator<StringBasedElement> MapParamName(this IAccumulator<StringBasedElement> acc, Func<string, string> map)
        {
            return acc.MapParam(_Map);
            StringBasedElement _Map(StringBasedElement el) => new StringBasedElement(el.ParamRoot, map(el.Param), el.AggregatedToTable);
        }

        public static string BuildFromString<TElement>(this IAccumulator<TElement> acc, BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias = null)
        {
            switch (acc)
            {
                case Accumulator<StringBasedElement> a:
                    return BuildFromString(a, state, sqlFragmentBuilder, wrappedQueryAlias, wrappedQueryAlias == null);
                case BinaryAccumulator<StringBasedElement> a:
                    return _BuildFromString(a, state, sqlFragmentBuilder, wrappedQueryAlias);
                case UnaryAccumulator<StringBasedElement> a:
                    return _BuildFromString(a, state, sqlFragmentBuilder, wrappedQueryAlias);
                case Accumulator<SelectColumnBasedElement> a:
                    return BuildFromString(a, state, sqlFragmentBuilder, wrappedQueryAlias);
                case BinaryAccumulator<SelectColumnBasedElement> a:
                    return _BuildFromString(a, state, sqlFragmentBuilder, wrappedQueryAlias);
                case UnaryAccumulator<SelectColumnBasedElement> a:
                    return _BuildFromString(a, state, sqlFragmentBuilder, wrappedQueryAlias);
                default:
                    throw new NotSupportedException($"IAccumulator<{typeof(TElement)}>");
            }
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
                    throw new InvalidOperationException($"Cannot build accumulator for expression type {combine}.");
            }
        }

        public static string AddUnaryCondition(ISqlSyntax sqlFragmentBuilder, string input, UnarySqlOperator condition, bool hasOneItemOnly)
        {
            switch (condition)
            {
                case UnarySqlOperator.AverageFunction:
                    return Func(sqlFragmentBuilder.AverageFunctionName);
                case UnarySqlOperator.CountFunction:
                    return Func(sqlFragmentBuilder.CountFunctionName);
                case UnarySqlOperator.MaxFunction:
                    return Func(sqlFragmentBuilder.MaxFunctionName);
                case UnarySqlOperator.MinFunction:
                    return Func(sqlFragmentBuilder.MinFunctionName);
                case UnarySqlOperator.SumFunction:
                    return Func(sqlFragmentBuilder.SumFunctionName);

                default:
                    throw new InvalidOperationException($"Cannot build accumulator for expression type {condition}.");
            }

            string Func(string functionName) => $"{functionName}({input})";
        }

        private static string BuildFromString(Accumulator<StringBasedElement> acc, BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias, bool tableIsFirstParamPart)
        {
            if (tableIsFirstParamPart && wrappedQueryAlias != null)
                throw new InvalidOperationException($"You cannot specify {nameof(wrappedQueryAlias)} and {nameof(tableIsFirstParamPart)}");

            var table1 = (acc.First.Param ?? "").StartsWith("@") ? null : wrappedQueryAlias;

            return acc.Next.Aggregate(
                BuildColumn(table1, acc.First),
                Aggregate);

            string Aggregate(string x, (StringBasedElement param, BinarySqlOperator type) y)
            {
                var table = (y.param.Param ?? "").StartsWith("@") ? null : wrappedQueryAlias;
                var yValue = BuildColumn(table, y.param);

                return Combine(sqlFragmentBuilder, x, yValue, y.type);
            }

            string BuildColumn(string tab, StringBasedElement el)
            {
                var column = el.AddRoot(state).param;
                if (tableIsFirstParamPart)
                {
                    var p = column.Split('.');
                    if (p.Length > 1)
                    {
                        tab = p.Take(p.Length - 1).JoinString(".");
                        column = p[p.Length - 1];
                    }
                }

                return sqlFragmentBuilder.BuildSelectColumn(tab, column);
            }
        }

        private static string BuildFromString(Accumulator<SelectColumnBasedElement> acc, BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias)
        {
            return acc.Next.Aggregate(
                BuildColumn(acc.First),
                Aggregate);

            string Aggregate(string x, (SelectColumnBasedElement param, BinarySqlOperator type) y)
            {
                var yValue = BuildColumn(y.param);

                return Combine(sqlFragmentBuilder, x, yValue, y.type);
            }

            string BuildColumn(SelectColumnBasedElement el)
            {
                return sqlFragmentBuilder.BuildSelectColumn(
                    el.IsParameter ? null : wrappedQueryAlias, 
                    el.IsParameter ? el.ParameterName : el.Column.Alias);
            }
        }
        
        static string _BuildFromString<TElement>(BinaryAccumulator<TElement> acc, BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias)
        {
            var first = wrappedQueryAlias != null 
                ? acc.First.BuildFromString(state, sqlFragmentBuilder, wrappedQueryAlias)
                : acc.First.BuildFromString(state, sqlFragmentBuilder);

            var second = wrappedQueryAlias != null 
                ? acc.Next.Item1.BuildFromString(state, sqlFragmentBuilder, wrappedQueryAlias)
                : acc.Next.Item1.BuildFromString(state, sqlFragmentBuilder);

            if (!acc.First.HasOneItemOnly) first = $"({first})";
            if (!acc.Next.Item1.HasOneItemOnly) second = $"({second})";

            return Combine(
                sqlFragmentBuilder,
                first, 
                second,
                acc.Next.Item2);
        }
        
        static string _BuildFromString<TElement>(UnaryAccumulator<TElement> acc, BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias)
        {
            var first = wrappedQueryAlias != null 
                ? acc.First.BuildFromString(state, sqlFragmentBuilder, wrappedQueryAlias)
                : acc.First.BuildFromString(state, sqlFragmentBuilder);

            return AddUnaryCondition(
                sqlFragmentBuilder,
                first, 
                acc.Operator,
                acc.First.HasOneItemOnly);
        }
    }
}