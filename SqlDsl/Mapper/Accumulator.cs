using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    class Accumulator : Accumulator<(ParameterExpression paramRoot, string param), ExpressionType>
    {   
        public Accumulator(
            ParameterExpression firstParamRoot, string firstParam, 
            IEnumerable<((ParameterExpression paramRoot, string param), ExpressionType)> next = null)
            : base((firstParamRoot, firstParam), next)
        {
        }
        
        public Accumulator(Accumulator<(ParameterExpression paramRoot, string param), ExpressionType> acc)
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

        public Accumulator Combine(Accumulator x, ExpressionType combiner)
        {
            return new Accumulator(base.Combine(x, combiner));
        }

        public string BuildFromString(BuildMapState state, ISqlFragmentBuilder sqlFragmentBuilder, string wrappedQueryAlias)
        {
            var table1 = (First.param ?? "").StartsWith("@") ? null : wrappedQueryAlias;

            return Next.Aggregate(
                BuildColumn(table1, First.paramRoot, First.param),
                Aggregate);

            string Aggregate(string x, ((ParameterExpression paramRoot, string param) param, ExpressionType type) y)
            {
                var table = (y.param.param ?? "").StartsWith("@") ? null : wrappedQueryAlias;
                var yValue = BuildColumn(table, y.param.paramRoot, y.param.param);

                switch (y.type)
                {
                    case ExpressionType.Add:
                        return sqlFragmentBuilder.BuildAddCondition(x, yValue);

                    case ExpressionType.Subtract:
                        return sqlFragmentBuilder.BuildSubtractCondition(x, yValue);
                        
                    case ExpressionType.Multiply:
                        return sqlFragmentBuilder.BuildMultiplyCondition(x, yValue);
                        
                    case ExpressionType.Divide:
                        return sqlFragmentBuilder.BuildDivideCondition(x, yValue);
                        
                    case ExpressionType.Equal:
                        return sqlFragmentBuilder.BuildEqualityCondition(x, yValue);
                        
                    case ExpressionType.NotEqual:
                        return sqlFragmentBuilder.BuildNonEqualityCondition(x, yValue);
                        
                    case ExpressionType.GreaterThan:
                        return sqlFragmentBuilder.BuildGreaterThanCondition(x, yValue);
                        
                    case ExpressionType.GreaterThanOrEqual:
                        return sqlFragmentBuilder.BuildGreaterThanEqualToCondition(x, yValue);
                        
                    case ExpressionType.LessThan:
                        return sqlFragmentBuilder.BuildLessThanCondition(x, yValue);
                        
                    case ExpressionType.LessThanOrEqual:
                        return sqlFragmentBuilder.BuildLessThanEqualToCondition(x, yValue);

                    default:
                        throw new InvalidOperationException($"Cannot build accumulator for expression type {y.type}.");
                }
            }

            string BuildColumn(string tab, ParameterExpression paramRoot, string parameter)
            {
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
}