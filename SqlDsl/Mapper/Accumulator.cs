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
        (ParameterExpression paramRoot, string param, string aggregatedToTable) First  { get; }
        IEnumerable<(ParameterExpression paramRoot, string param, string aggregatedToTable)> GetEnumerable1();
        IAccumulator MapParam(Func<(ParameterExpression paramRoot, string param, string aggregatedToTable), (ParameterExpression, string, string)> map);
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

        (ParameterExpression paramRoot, string param, string aggregatedToTable) IAccumulator.First => First.First;

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

        public IEnumerable<(ParameterExpression paramRoot, string param, string aggregatedToTable)> GetEnumerable1()
        {
            return First
                .GetEnumerable1()
                .Concat(Next.Item1.GetEnumerable1());
        }

        public IAccumulator MapParam(Func<(ParameterExpression paramRoot, string param, string aggregatedToTable), (ParameterExpression, string, string)> map)
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
        
        public (ParameterExpression paramRoot, string param, string aggregatedToTable) First => Inner.First;

        readonly Accumulator<(ParameterExpression paramRoot, string param, string aggregatedToTable), CombinationType> Inner;

        public Accumulator(
            ParameterExpression firstParamRoot, string firstParam, string aggregatedToTable, 
            IEnumerable<((ParameterExpression paramRoot, string param, string aggregatedToTable), CombinationType)> next = null)
            : this(new Accumulator<(ParameterExpression paramRoot, string param, string aggregatedToTable), CombinationType>((firstParamRoot, firstParam, aggregatedToTable), next))
        {
        }
        
        public Accumulator(Accumulator<(ParameterExpression paramRoot, string param, string aggregatedToTable), CombinationType> acc)
        {
            Inner = acc;
        }

        public IEnumerable<(ParameterExpression paramRoot, string param, string aggregatedToTable)> GetEnumerable1()
        {
            return Inner.GetEnumerable1();
        }


        public IAccumulator MapParam(Func<(ParameterExpression paramRoot, string param, string aggregatedToTable), (ParameterExpression, string, string)> map)
        {
            return new Accumulator(Inner.Map(map));
        }

        public IAccumulator MapParamName(Func<string, string> map)
        {
            return MapParam(_Map);
            (ParameterExpression, string, string) _Map((ParameterExpression x, string y, string z) w) => (w.x, map(w.y), w.z);
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
                BuildColumn(table1, Inner.First.paramRoot, Inner.First.param, Inner.First.aggregatedToTable),
                Aggregate);

            string Aggregate(string x, ((ParameterExpression paramRoot, string param, string aggregatedToTable) param, CombinationType type) y)
            {
                var table = (y.param.param ?? "").StartsWith("@") ? null : wrappedQueryAlias;
                var yValue = BuildColumn(table, y.param.paramRoot, y.param.param, y.param.aggregatedToTable);

                return Combine(sqlFragmentBuilder, x, yValue, y.type);
            }

            string BuildColumn(string tab, ParameterExpression paramRoot, string parameter, string aggregatedToTable)
            {
                //SqlStatementConstants.OpenFunctionAlias
                var p = parameter.Split('.');
                for (var i = 0; i < p.Length - 1; i++)
                {
                    if (p[i].StartsWith(SqlStatementConstants.OpenFunctionAlias))
                        throw new NotSupportedException("You can only have one function per column reference.");
                }

                string func = null;
                if (p[p.Length - 1].StartsWith(SqlStatementConstants.OpenFunctionAlias))
                {
                    func = p[p.Length - 1].Substring(SqlStatementConstants.OpenFunctionAlias.Length);
                    parameter = p.Take(p.Length - 1).JoinString(".");
                }
                else
                {
                    parameter = p.JoinString(".");
                }

                if (tableIsFirstParamPart)
                {
                    p = parameter.Split('.');
                    if (p.Length > 1)
                    {
                        tab = p.Take(p.Length - 1).JoinString(".");
                        parameter = p[p.Length - 1];
                    }
                }

                var col = sqlFragmentBuilder.BuildSelectColumn(tab, AddRoot(paramRoot, parameter, aggregatedToTable, state).param);
                return func == null ?
                    col :
                    $"{func}({col})";   // TODO: call func in sqlBuilder
            }
        }

        public static Func<(ParameterExpression, string, string), (string param, string aggregatedToTable)> AddRoot(BuildMapState state)
        {
            return Execute;

            (string, string) Execute((ParameterExpression root, string property, string aggregatedToTable) x)
            {
                var (root, property, _) = x;

                // I am not 100% sure about the "root == state.QueryObject" part
                if (root == null || root == state.QueryObject) return (property, x.aggregatedToTable);

                var propertyRoot = state.ParameterRepresentsProperty
                    .Where(p => p.parameter == root)
                    .Select(p => p.property.JoinString("."))
                    .FirstOrDefault();

                if (propertyRoot == null)
                    throw new InvalidOperationException($"Could not find alias for mapping parameter \"{root}\". {property}");

                if (!string.IsNullOrEmpty(property))
                    propertyRoot += ".";

                return ($"{propertyRoot}{property}", x.aggregatedToTable);
            }
        }

        public static (string param, string aggregatedToTable) AddRoot(ParameterExpression root, string property, string aggregatedToTable, BuildMapState state)
        {
            return AddRoot(state)((root, property, aggregatedToTable));
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