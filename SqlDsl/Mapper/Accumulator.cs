using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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

    struct Element
    {
        public readonly ParameterExpression ParamRoot;
        public readonly string Param;
        public readonly string AggregatedToTable;
        public readonly string Function;

        public Element(ParameterExpression paramRoot, string param, string aggregatedToTable, string function)
        {
            ParamRoot = paramRoot;
            Param = param;
            AggregatedToTable = aggregatedToTable;
            Function = function;
        }
    }

    struct TheAmazingElement
    {
        public bool IsParameter => _ParameterName != null;
        readonly string _ParameterName;
        public string ParameterName => EnsureParam(_ParameterName);
        readonly ISelectColumn _Column;
        public ISelectColumn Column => EnsureCol(_Column);
        readonly ISelectColumn _RowIdColumn;
        public ISelectColumn RowIdColumn => EnsureCol(_RowIdColumn);
        public readonly string Function;

        /// <summary>
        /// If true, the colum and row id columns come from different tables
        /// </summary>
        public bool ColumnIsAggregatedToDifferentTable => Column.RowNumberColumnIndex != RowIdColumn.RowNumberColumnIndex;

        public TheAmazingElement(ISelectColumn column, ISelectColumn rowIdColumn, string function)
        {
            _Column = column ?? throw new ArgumentNullException(nameof(column));
            _RowIdColumn = rowIdColumn ?? throw new ArgumentNullException(nameof(rowIdColumn));
            Function = function;
            
            _ParameterName = null;
        }

        public TheAmazingElement(string parameterName, string function)
        {
            _ParameterName = parameterName ?? throw new ArgumentNullException(nameof(parameterName));
            Function = function;
            
            _Column = null;
            _RowIdColumn = null;
        }

        static T EnsureCol<T>(T value) where T: class => value ?? throw new InvalidOperationException("This value is only available if IsParameter == false");
        
        static T EnsureParam<T>(T value) where T: class => value ?? throw new InvalidOperationException("This value is only available if IsParameter == true");
    }

    class Accumulators<TElement> : IAccumulator<TElement>
    {
        public readonly IAccumulator<TElement> First;
        public readonly (IAccumulator<TElement>, CombinationType) Next;

        public Accumulators(IAccumulator<TElement> first, (IAccumulator<TElement>, CombinationType) next)
        {
            First = first;
            Next = next;
        }

        public bool HasOneItemOnly => false;

        TElement IAccumulator<TElement>.First => First.First;

        public IAccumulator<TElement> Combine(IAccumulator<TElement> x, CombinationType combiner)
        {
            return new Accumulators<TElement>(this, (x, combiner));
        }

        public IEnumerable<TElement> GetEnumerable1()
        {
            return First
                .GetEnumerable1()
                .Concat(Next.Item1.GetEnumerable1());
        }

        public IAccumulator<T> MapParam<T>(Func<TElement, T> map)
        {
            var first = First.MapParam(map);
            var second = Next.Item1.MapParam(map);

            return first.Combine(second, Next.Item2);
        }
    }

    class Accumulator<TElement>: IAccumulator<TElement>
    {   
        public bool HasOneItemOnly => !Inner.Next.Any();
        
        public TElement First => Inner.First;
        
        public IEnumerable<(TElement element, CombinationType combiner)> Next => Inner.Next;

        readonly Accumulator<TElement, CombinationType> Inner;
        
        public Accumulator(Accumulator<TElement, CombinationType> acc)
        {
            Inner = acc;
        }

        public IEnumerable<TElement> GetEnumerable1()
        {
            return Inner.GetEnumerable1();
        }


        public IAccumulator<T> MapParam<T>(Func<TElement, T> map)
        {
            return new Accumulator<T>(Inner.Map(map));
        }

        public IAccumulator<TElement> Combine(IAccumulator<TElement> x, CombinationType combiner)
        {
            return new Accumulators<TElement>(this, (x, combiner));
        }
    }

    // todo: move file
    static class IAccumulatorUtils
    {
        // todo: move file??
        public static Func<Element, (string param, string aggregatedToTable)> AddRoot(BuildMapState state)
        {
            return Execute;

            (string, string) Execute(Element x)
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

        public static (string param, string aggregatedToTable) AddRoot(this Element value, BuildMapState state) => AddRoot(state)(value);

        public static IAccumulator<TheAmazingElement> Convert(this IAccumulator<Element> acc, BuildMapState state)
        {
            return acc.MapParam(Map);

            TheAmazingElement Map(Element el)
            {
                var (fullName, overrideTable) = el.AddRoot(state);
                if (fullName.StartsWith("@")) return new TheAmazingElement(fullName, el.Function);

                var col = state.WrappedSqlStatement.SelectColumns[fullName];
                var tab = state.WrappedSqlStatement.Tables[overrideTable ?? GetTableName(fullName)];
                var rid = state.WrappedSqlStatement.SelectColumns[tab.RowNumberColumnIndex];

                return new TheAmazingElement(col, rid, el.Function);
            }
        }

        static string GetTableName(string fullName)
        {
            var i = fullName.LastIndexOf('.');
            return i == -1 ?
                SqlStatementConstants.RootObjectAlias : 
                fullName.Substring(0, i);
        }

        public static IAccumulator<Element> MapParamName(this IAccumulator<Element> acc, Func<string, string> map)
        {
            return acc.MapParam(_Map);
            Element _Map(Element el) => new Element(el.ParamRoot, map(el.Param), el.AggregatedToTable, el.Function);
        }

        public static string BuildFromString<TElement>(this IAccumulator<TElement> acc, BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias = null)
        {
            switch (acc)
            {
                case Accumulator<Element> a:
                    return BuildFromString(a, state, sqlFragmentBuilder, wrappedQueryAlias, wrappedQueryAlias == null);
                case Accumulators<Element> a:
                    return _BuildFromString(a, state, sqlFragmentBuilder, wrappedQueryAlias);
                case Accumulator<TheAmazingElement> a:
                    return BuildFromString(a, state, sqlFragmentBuilder, wrappedQueryAlias);
                case Accumulators<TheAmazingElement> a:
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

        private static string BuildFromString(Accumulator<Element> acc, BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias, bool tableIsFirstParamPart)
        {
            if (tableIsFirstParamPart && wrappedQueryAlias != null)
                throw new InvalidOperationException($"You cannot specify {nameof(wrappedQueryAlias)} and {nameof(tableIsFirstParamPart)}");

            var table1 = (acc.First.Param ?? "").StartsWith("@") ? null : wrappedQueryAlias;

            return acc.Next.Aggregate(
                BuildColumn(table1, acc.First),
                Aggregate);

            string Aggregate(string x, (Element param, CombinationType type) y)
            {
                var table = (y.param.Param ?? "").StartsWith("@") ? null : wrappedQueryAlias;
                var yValue = BuildColumn(table, y.param);

                return Combine(sqlFragmentBuilder, x, yValue, y.type);
            }

            string BuildColumn(string tab, Element el)
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

        private static string BuildFromString(Accumulator<TheAmazingElement> acc, BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias)
        {
            return acc.Next.Aggregate(
                BuildColumn(acc.First),
                Aggregate);

            string Aggregate(string x, (TheAmazingElement param, CombinationType type) y)
            {
                var yValue = BuildColumn(y.param);

                return Combine(sqlFragmentBuilder, x, yValue, y.type);
            }

            string BuildColumn(TheAmazingElement el)
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

    // todo: move file
    public enum CombinationType
    {
        Add,
        Subtract,
        Multiply,
        Divide,
        In,
        Comma,
        Equal,
        NotEqual,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
        And,
        Or
    }

    // todo: move file
    public static class CombinationTypeUtils
    {
        public static CombinationType ToCombinationType(this ExpressionType e)
        {
            switch (e)
            {
                case ExpressionType.AndAlso:
                    return CombinationType.And;
                case ExpressionType.OrElse:
                    return CombinationType.Or;
                case ExpressionType.Add:
                    return CombinationType.Add;
                case ExpressionType.Subtract:
                    return CombinationType.Subtract;
                case ExpressionType.Multiply:
                    return CombinationType.Multiply;
                case ExpressionType.Divide:
                    return CombinationType.Divide;
                case ExpressionType.Equal:
                    return CombinationType.Equal;
                case ExpressionType.NotEqual:
                    return CombinationType.NotEqual;
                case ExpressionType.GreaterThan:
                    return CombinationType.GreaterThan;
                case ExpressionType.GreaterThanOrEqual:
                    return CombinationType.GreaterThanOrEqual;
                case ExpressionType.LessThan:
                    return CombinationType.LessThan;
                case ExpressionType.LessThanOrEqual:
                    return CombinationType.LessThanOrEqual;
                default:
                    throw new NotSupportedException(e.ToString());
            }
        }
    }
}