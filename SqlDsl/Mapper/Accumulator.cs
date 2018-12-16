using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    class Accumulator : Accumulator<(ParameterExpression paramRoot, string param), CombinationType>
    {   
        public Accumulator(
            ParameterExpression firstParamRoot, string firstParam, 
            IEnumerable<((ParameterExpression paramRoot, string param), CombinationType)> next = null)
            : base((firstParamRoot, firstParam), next)
        {
        }
        
        public Accumulator(Accumulator<(ParameterExpression paramRoot, string param), CombinationType> acc)
            : this(acc.First.paramRoot, acc.First.param, acc.Next)
        {
        }

        public Accumulator MapParam(Func<(ParameterExpression paramRoot, string param), (ParameterExpression, string)> map)
        {
            return new Accumulator(base.Map(map));
        }

        public Accumulator MapParamName(Func<string, string> map)
        {
            return MapParam(_Map);
            (ParameterExpression, string) _Map((ParameterExpression x, string y) z) => (z.x, map(z.y));
        }

        public Accumulator Combine(Accumulator x, CombinationType combiner)
        {
            return new Accumulator(base.Combine(x, combiner));
        }

        public string BuildFromString(BuildMapState state, ISqlFragmentBuilder sqlFragmentBuilder, string wrappedQueryAlias)
        {
            return BuildFromString(state, sqlFragmentBuilder, wrappedQueryAlias, false);
        }

        public string BuildFromString(BuildMapState state, ISqlFragmentBuilder sqlFragmentBuilder)
        {
            return BuildFromString(state, sqlFragmentBuilder, null, true);
        }

        private string BuildFromString(BuildMapState state, ISqlFragmentBuilder sqlFragmentBuilder, string wrappedQueryAlias, bool tableIsFirstParamPart)
        {
            if (tableIsFirstParamPart && wrappedQueryAlias != null)
                throw new InvalidOperationException($"You cannot specify {nameof(wrappedQueryAlias)} and {nameof(tableIsFirstParamPart)}");

            var table1 = (First.param ?? "").StartsWith("@") ? null : wrappedQueryAlias;

            return Next.Aggregate(
                BuildColumn(table1, First.paramRoot, First.param),
                Aggregate);

            string Aggregate(string x, ((ParameterExpression paramRoot, string param) param, CombinationType type) y)
            {
                var table = (y.param.param ?? "").StartsWith("@") ? null : wrappedQueryAlias;
                var yValue = BuildColumn(table, y.param.paramRoot, y.param.param);

                switch (y.type)
                {
                    case CombinationType.Add:
                        return sqlFragmentBuilder.BuildAddCondition(x, yValue);

                    case CombinationType.Subtract:
                        return sqlFragmentBuilder.BuildSubtractCondition(x, yValue);
                        
                    case CombinationType.Multiply:
                        return sqlFragmentBuilder.BuildMultiplyCondition(x, yValue);
                        
                    case CombinationType.Divide:
                        return sqlFragmentBuilder.BuildDivideCondition(x, yValue);
                        
                    case CombinationType.In:
                        return sqlFragmentBuilder.BuildInCondition(x, yValue);
                        
                    case CombinationType.Comma:
                        return sqlFragmentBuilder.BuildCommaCondition(x, yValue);
                        
                    case CombinationType.Equal:
                        return sqlFragmentBuilder.BuildEqualityCondition(x, yValue);
                        
                    case CombinationType.NotEqual:
                        return sqlFragmentBuilder.BuildNonEqualityCondition(x, yValue);
                        
                    case CombinationType.GreaterThan:
                        return sqlFragmentBuilder.BuildGreaterThanCondition(x, yValue);
                        
                    case CombinationType.GreaterThanOrEqual:
                        return sqlFragmentBuilder.BuildGreaterThanEqualToCondition(x, yValue);
                        
                    case CombinationType.LessThan:
                        return sqlFragmentBuilder.BuildLessThanCondition(x, yValue);
                        
                    case CombinationType.LessThanOrEqual:
                        return sqlFragmentBuilder.BuildLessThanEqualToCondition(x, yValue);

                    default:
                        throw new InvalidOperationException($"Cannot build accumulator for expression type {y.type}.");
                }
            }

            string BuildColumn(string tab, ParameterExpression paramRoot, string parameter)
            {
                if (tableIsFirstParamPart)
                {
                    var p = parameter.Split('.');
                    if (p.Length > 1)
                    {
                        tab = p[0];
                        parameter = p.Skip(1).JoinString(".");
                    }
                }

                return sqlFragmentBuilder.BuildSelectColumn(tab, AddRoot(paramRoot, parameter, state));
            }
        }

        public static Func<(ParameterExpression, string), string> AddRoot(BuildMapState state)
        {
            return Execute;

            string Execute((ParameterExpression root, string property) x)
            {
                var (root, property) = x;

                // I am not 100% sure about the "root == state.QueryObject" part
                if (root == null || root == state.QueryObject) return property;

                var propertyRoot = state.ParameterRepresentsProperty
                    .Where(p => p.parameter == root)
                    .Select(p => p.property.JoinString("."))
                    .FirstOrDefault();

                if (propertyRoot == null)
                    throw new InvalidOperationException($"Could not find alias for mapping parameter \"{root}\".");

                if (!string.IsNullOrEmpty(property))
                    propertyRoot += ".";

                return $"{propertyRoot}{property}";
            }
        }

        public static string AddRoot(ParameterExpression root, string property, BuildMapState state)
        {
            return AddRoot(state)((root, property));
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
        LessThanOrEqual
    }

    public static class CombinationTypeUtils
    {
        public static CombinationType ToCombinationType(this ExpressionType e)
        {
            switch (e)
            {
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