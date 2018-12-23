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
        (ParameterExpression paramRoot, string param, bool isAggregate) First  { get; }
        IEnumerable<(ParameterExpression paramRoot, string param, bool isAggregate)> GetEnumerable1();
        IAccumulator MapParam(Func<(ParameterExpression paramRoot, string param, bool isAggregate), (ParameterExpression, string, bool)> map);
        IAccumulator MapParamName(Func<string, string> map);
        IAccumulator Combine(IAccumulator x, CombinationType combiner);
        string BuildFromString(BuildMapState state, ISqlFragmentBuilder sqlFragmentBuilder, string wrappedQueryAlias);
        string BuildFromString(BuildMapState state, ISqlFragmentBuilder sqlFragmentBuilder);
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

        (ParameterExpression paramRoot, string param, bool isAggregate) IAccumulator.First => First.First;

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

        public IEnumerable<(ParameterExpression paramRoot, string param, bool isAggregate)> GetEnumerable1()
        {
            return First
                .GetEnumerable1()
                .Concat(Next.Item1.GetEnumerable1());
        }

        public IAccumulator MapParam(Func<(ParameterExpression paramRoot, string param, bool isAggregate), (ParameterExpression, string, bool)> map)
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
        
        public (ParameterExpression paramRoot, string param, bool isAggregate) First => Inner.First;

        readonly Accumulator<(ParameterExpression paramRoot, string param, bool isAggregate), CombinationType> Inner;

        public Accumulator(
            ParameterExpression firstParamRoot, string firstParam, bool isAggregate, 
            IEnumerable<((ParameterExpression paramRoot, string param, bool isAggregate), CombinationType)> next = null)
            : this(new Accumulator<(ParameterExpression paramRoot, string param, bool isAggregate), CombinationType>((firstParamRoot, firstParam, isAggregate), next))
        {
        }
        
        public Accumulator(Accumulator<(ParameterExpression paramRoot, string param, bool isAggregate), CombinationType> acc)
        {
            Inner = acc;
        }

        public IEnumerable<(ParameterExpression paramRoot, string param, bool isAggregate)> GetEnumerable1()
        {
            return Inner.GetEnumerable1();
        }


        public IAccumulator MapParam(Func<(ParameterExpression paramRoot, string param, bool isAggregate), (ParameterExpression, string, bool)> map)
        {
            return new Accumulator(Inner.Map(map));
        }

        public IAccumulator MapParamName(Func<string, string> map)
        {
            return MapParam(_Map);
            (ParameterExpression, string, bool) _Map((ParameterExpression x, string y, bool z) w) => (w.x, map(w.y), w.z);
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

            var table1 = (Inner.First.param ?? "").StartsWith("@") ? null : wrappedQueryAlias;

            return Inner.Next.Aggregate(
                BuildColumn(table1, Inner.First.paramRoot, Inner.First.param, Inner.First.isAggregate),
                Aggregate);

            string Aggregate(string x, ((ParameterExpression paramRoot, string param, bool isAggregate) param, CombinationType type) y)
            {
                var table = (y.param.param ?? "").StartsWith("@") ? null : wrappedQueryAlias;
                var yValue = BuildColumn(table, y.param.paramRoot, y.param.param, y.param.isAggregate);

                return Combine(sqlFragmentBuilder, x, yValue, y.type);
            }

            string BuildColumn(string tab, ParameterExpression paramRoot, string parameter, bool isAggregate)
            {
                if (tableIsFirstParamPart)
                {
                    var p = parameter.Split('.');
                    if (p.Length > 1)
                    {
                        tab = p.Take(p.Length - 1).JoinString(".");
                        parameter = p[p.Length - 1];
                    }
                }

                return sqlFragmentBuilder.BuildSelectColumn(tab, AddRoot(paramRoot, parameter, isAggregate, state).param);
            }
        }

        public static Func<(ParameterExpression, string, bool), (string param, bool isAggregate)> AddRoot(BuildMapState state)
        {
            return Execute;

            (string , bool) Execute((ParameterExpression root, string property, bool isAggregate) x)
            {
                var (root, property, _) = x;

                // I am not 100% sure about the "root == state.QueryObject" part
                if (root == null || root == state.QueryObject) return (property, x.isAggregate);

                var propertyRoot = state.ParameterRepresentsProperty
                    .Where(p => p.parameter == root)
                    .Select(p => p.property.JoinString("."))
                    .FirstOrDefault();

                if (propertyRoot == null)
                    throw new InvalidOperationException($"Could not find alias for mapping parameter \"{root}\". {property}");

                if (!string.IsNullOrEmpty(property))
                    propertyRoot += ".";

                return ($"{propertyRoot}{property}", x.isAggregate);
            }
        }

        public static (string param, bool isAggregate) AddRoot(ParameterExpression root, string property, bool isAggregate, BuildMapState state)
        {
            return AddRoot(state)((root, property, isAggregate));
        }
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