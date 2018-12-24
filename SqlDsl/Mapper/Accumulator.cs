using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    interface IAccumulator
    {
        bool HasOneItemOnly { get; }
        Element First  { get; }
        IEnumerable<Element> GetEnumerable1();
        IAccumulator MapParam(Func<Element, Element> map);
        IAccumulator MapParamName(Func<string, string> map);
        IAccumulator Combine(IAccumulator x, CombinationType combiner);
        string BuildFromString(BuildMapState state, ISqlFragmentBuilder sqlFragmentBuilder, string wrappedQueryAlias);
        string BuildFromString(BuildMapState state, ISqlFragmentBuilder sqlFragmentBuilder);
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

    class Accumulators : IAccumulator
    {
        readonly IAccumulator First;
        readonly (IAccumulator, CombinationType) Next;

        public Accumulators(IAccumulator first, (IAccumulator, CombinationType) next)
        {
            First = first;
            Next = next;
        }

        public bool HasOneItemOnly => false;

        Element IAccumulator.First => First.First;

        public string BuildFromString(BuildMapState state, ISqlFragmentBuilder sqlFragmentBuilder, string wrappedQueryAlias)
        {
            // todo: put string part in ISqlFragmentBuilder

            var first = First.BuildFromString(state, sqlFragmentBuilder, wrappedQueryAlias);
            var second = Next.Item1.BuildFromString(state, sqlFragmentBuilder, wrappedQueryAlias);

            return BuildFromString(sqlFragmentBuilder, first, second);
        }

        public string BuildFromString(BuildMapState state, ISqlFragmentBuilder sqlFragmentBuilder)
        {
            // todo: put string part in ISqlFragmentBuilder

            var first = First.BuildFromString(state, sqlFragmentBuilder);
            var second = Next.Item1.BuildFromString(state, sqlFragmentBuilder);

            return BuildFromString(sqlFragmentBuilder, first, second);
        }

        string BuildFromString(ISqlFragmentBuilder sqlFragmentBuilder, string first, string next)
        {
            if (!First.HasOneItemOnly) first = $"({first})";
            if (!Next.Item1.HasOneItemOnly) next = $"({next})";

            return Accumulator.Combine(
                sqlFragmentBuilder,
                first, 
                next,
                Next.Item2);
        }

        public IAccumulator Combine(IAccumulator x, CombinationType combiner)
        {
            return new Accumulators(this, (x, combiner));
        }

        public IEnumerable<Element> GetEnumerable1()
        {
            return First
                .GetEnumerable1()
                .Concat(Next.Item1.GetEnumerable1());
        }

        public IAccumulator MapParam(Func<Element, Element> map)
        {
            var first = First.MapParam(map);
            var second = Next.Item1.MapParam(map);

            return first.Combine(second, Next.Item2);
        }

        public IAccumulator MapParamName(Func<string, string> map)
        {
            var first = First.MapParamName(map);
            var second = Next.Item1.MapParamName(map);

            return first.Combine(second, Next.Item2);
        }
    }

    class Accumulator: IAccumulator
    {   
        public bool HasOneItemOnly => !Inner.Next.Any();
        
        public Element First => Inner.First;

        readonly Accumulator<Element, CombinationType> Inner;

        public Accumulator(
            ParameterExpression firstParamRoot, string firstParam, string aggregatedToTable, string function, 
            IEnumerable<(Element, CombinationType)> next = null)
            : this(new Accumulator<Element, CombinationType>(new Element(firstParamRoot, firstParam, aggregatedToTable, function), next))
        {
        }
        
        public Accumulator(Accumulator<Element, CombinationType> acc)
        {
            Inner = acc;
        }

        public IEnumerable<Element> GetEnumerable1()
        {
            return Inner.GetEnumerable1();
        }


        public IAccumulator MapParam(Func<Element, Element> map)
        {
            return new Accumulator(Inner.Map(map));
        }

        public IAccumulator MapParamName(Func<string, string> map)
        {
            return MapParam(_Map);
            Element _Map(Element el) => new Element(el.ParamRoot, map(el.Param), el.AggregatedToTable, el.Function);
        }

        public IAccumulator Combine(IAccumulator x, CombinationType combiner)
        {
            return new Accumulators(this, (x, combiner));
        }

        public string BuildFromString(BuildMapState state, ISqlFragmentBuilder sqlFragmentBuilder, string wrappedQueryAlias)
        {
            return BuildFromString(state, sqlFragmentBuilder, wrappedQueryAlias, false);
        }

        public string BuildFromString(BuildMapState state, ISqlFragmentBuilder sqlFragmentBuilder)
        {
            return BuildFromString(state, sqlFragmentBuilder, null, true);
        }

        public static string Combine(ISqlFragmentBuilder sqlFragmentBuilder, string l, string r, CombinationType combine)
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

        private string BuildFromString(BuildMapState state, ISqlFragmentBuilder sqlFragmentBuilder, string wrappedQueryAlias, bool tableIsFirstParamPart)
        {
            if (tableIsFirstParamPart && wrappedQueryAlias != null)
                throw new InvalidOperationException($"You cannot specify {nameof(wrappedQueryAlias)} and {nameof(tableIsFirstParamPart)}");

            var table1 = (Inner.First.Param ?? "").StartsWith("@") ? null : wrappedQueryAlias;

            return Inner.Next.Aggregate(
                BuildColumn(table1, Inner.First),
                Aggregate);

            string Aggregate(string x, (Element param, CombinationType type) y)
            {
                var table = (y.param.Param ?? "").StartsWith("@") ? null : wrappedQueryAlias;
                var yValue = BuildColumn(table, y.param);

                return Combine(sqlFragmentBuilder, x, yValue, y.type);
            }

            string BuildColumn(string tab, Element el)
            {
                var parameter = el.Param;

                if (tableIsFirstParamPart)
                {
                    var p = parameter.Split('.');
                    if (p.Length > 1)
                    {
                        tab = p.Take(p.Length - 1).JoinString(".");
                        parameter = p[p.Length - 1];
                    }
                }

                var col = sqlFragmentBuilder.BuildSelectColumn(tab, AddRoot(new Element(el.ParamRoot, parameter, el.AggregatedToTable, el.Function), state).param);
                return el.Function == null ?
                    col :
                    $"{el.Function}({col})";   // TODO: call func in sqlBuilder
            }
        }

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

        public static (string param, string aggregatedToTable) AddRoot(Element value, BuildMapState state) => AddRoot(state)(value);
    }

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