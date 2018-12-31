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
        IEnumerable<TElement> GetEnumerable1();
        IAccumulator<T> MapParam<T>(Func<TElement, T> map);
        IAccumulator<TElement> Combine(IAccumulator<TElement> x, CombinationType combiner);
    }

    static class IAccumulatorUtils
    {
        // todo: move file??
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
            return acc.MapParam(Map);

            SelectColumnBasedElement Map(StringBasedElement el)
            {
                var (fullName, overrideTable) = el.AddRoot(state);
                if (fullName.StartsWith("@")) return new SelectColumnBasedElement(fullName, el.Function);

                var col = state.WrappedSqlStatement.SelectColumns[fullName];
                var tab = state.WrappedSqlStatement.Tables[overrideTable ?? GetTableName(fullName)];
                var rid = tab.RowNumberColumn;

                return new SelectColumnBasedElement(col, rid, el.Function);
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
            StringBasedElement _Map(StringBasedElement el) => new StringBasedElement(el.ParamRoot, map(el.Param), el.AggregatedToTable, el.Function);
        }

        public static string BuildFromString<TElement>(this IAccumulator<TElement> acc, BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias = null)
        {
            switch (acc)
            {
                case Accumulator<StringBasedElement> a:
                    return BuildFromString(a, state, sqlFragmentBuilder, wrappedQueryAlias, wrappedQueryAlias == null);
                case Accumulators<StringBasedElement> a:
                    return _BuildFromString(a, state, sqlFragmentBuilder, wrappedQueryAlias);
                case Accumulator<SelectColumnBasedElement> a:
                    return BuildFromString(a, state, sqlFragmentBuilder, wrappedQueryAlias);
                case Accumulators<SelectColumnBasedElement> a:
                    return _BuildFromString(a, state, sqlFragmentBuilder, wrappedQueryAlias);
                default:
                    throw new NotSupportedException($"IAccumulator<{typeof(TElement)}>");
            }
        }

        public static string Combine(ISqlSyntax sqlFragmentBuilder, string l, string r, CombinationType combine)
        {
            switch (combine)
            {
                case CombinationType.And:
                    return sqlFragmentBuilder.BuildAndCondition(l, r);
                    
                case CombinationType.Or:
                    return sqlFragmentBuilder.BuildOrCondition(l, r);
                    
                case CombinationType.Add:
                    return sqlFragmentBuilder.BuildAddCondition(l, r);

                case CombinationType.Subtract:
                    return sqlFragmentBuilder.BuildSubtractCondition(l, r);
                    
                case CombinationType.Multiply:
                    return sqlFragmentBuilder.BuildMultiplyCondition(l, r);
                    
                case CombinationType.Divide:
                    return sqlFragmentBuilder.BuildDivideCondition(l, r);
                    
                case CombinationType.In:
                    return sqlFragmentBuilder.BuildInCondition(l, r);
                    
                case CombinationType.Comma:
                    return sqlFragmentBuilder.BuildCommaCondition(l, r);
                    
                case CombinationType.Equal:
                    return sqlFragmentBuilder.BuildEqualityCondition(l, r);
                    
                case CombinationType.NotEqual:
                    return sqlFragmentBuilder.BuildNonEqualityCondition(l, r);
                    
                case CombinationType.GreaterThan:
                    return sqlFragmentBuilder.BuildGreaterThanCondition(l, r);
                    
                case CombinationType.GreaterThanOrEqual:
                    return sqlFragmentBuilder.BuildGreaterThanEqualToCondition(l, r);
                    
                case CombinationType.LessThan:
                    return sqlFragmentBuilder.BuildLessThanCondition(l, r);
                    
                case CombinationType.LessThanOrEqual:
                    return sqlFragmentBuilder.BuildLessThanEqualToCondition(l, r);

                default:
                    throw new InvalidOperationException($"Cannot build accumulator for expression type {combine}.");
            }
        }

        private static string BuildFromString(Accumulator<StringBasedElement> acc, BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias, bool tableIsFirstParamPart)
        {
            if (tableIsFirstParamPart && wrappedQueryAlias != null)
                throw new InvalidOperationException($"You cannot specify {nameof(wrappedQueryAlias)} and {nameof(tableIsFirstParamPart)}");

            var table1 = (acc.First.Param ?? "").StartsWith("@") ? null : wrappedQueryAlias;

            return acc.Next.Aggregate(
                BuildColumn(table1, acc.First),
                Aggregate);

            string Aggregate(string x, (StringBasedElement param, CombinationType type) y)
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

                var col = sqlFragmentBuilder.BuildSelectColumn(tab, column);
                return el.Function == null ?
                    col :
                    $"{el.Function}({col})";   // TODO: call func in sqlBuilder
            }
        }

        private static string BuildFromString(Accumulator<SelectColumnBasedElement> acc, BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias)
        {
            return acc.Next.Aggregate(
                BuildColumn(acc.First),
                Aggregate);

            string Aggregate(string x, (SelectColumnBasedElement param, CombinationType type) y)
            {
                var yValue = BuildColumn(y.param);

                return Combine(sqlFragmentBuilder, x, yValue, y.type);
            }

            string BuildColumn(SelectColumnBasedElement el)
            {
                var col = sqlFragmentBuilder.BuildSelectColumn(
                    el.IsParameter ? null : wrappedQueryAlias, 
                    el.IsParameter ? el.ParameterName : el.Column.Alias);

                return el.Function == null ?
                    col :
                    $"{el.Function}({col})";   // TODO: call func in sqlBuilder
            }
        }
        
        static string _BuildFromString<TElement>(Accumulators<TElement> acc, BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias)
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
    }
}