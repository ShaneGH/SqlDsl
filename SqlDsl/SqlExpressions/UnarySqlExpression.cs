using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using SqlDsl.Mapper;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using SqlDsl.Utils.Diagnostics;

namespace SqlDsl.SqlExpressions
{
    [DebuggerDisplay("{GetDebuggerDisplay()}")]
    class UnarySqlExpression<TElement> : ISqlExpression<TElement>,  IDebuggerDisplay
    {
        public readonly ISqlExpression<TElement> First;
        public readonly UnarySqlOperator Operator;
        public AggregationType AggregationType => GetAggregationType();

        public UnarySqlExpression(ISqlExpression<TElement> first, UnarySqlOperator op)
        {
            First = first;
            Operator = op;
        }

        public bool HasOneItemOnly => First.HasOneItemOnly;

        TElement ISqlExpression<TElement>.First => First.First;

        public ISqlExpression<TElement> Combine(ISqlExpression<TElement> x, BinarySqlOperator combiner)
        {
            return new BinarySqlExpression<TElement>(this, x, combiner);
        }

        public IEnumerable<TElement> GetEnumerable()
        {
            return First.GetEnumerable();
        }

        public IEnumerable<(bool isAggregated, TElement element)> GetAggregatedEnumerable()
        {
            bool isAggregated = AggregationType == AggregationType.IsAggregated;
            
            return First
                .GetAggregatedEnumerable()
                .Select(x => (x.isAggregated || isAggregated, x.element));
        }

        public ISqlExpression<T> MapParam<T>(Func<TElement, T> map)
        {
            var first = First.MapParam(map);
            return new UnarySqlExpression<T>(first, Operator);
        }

        public AggregationType GetAggregationType()
        {
            switch (Operator)
            {
                case UnarySqlOperator.AverageFunction:
                case UnarySqlOperator.CountFunction:
                case UnarySqlOperator.MaxFunction:
                case UnarySqlOperator.MinFunction:
                case UnarySqlOperator.SumFunction:
                    return AggregationType.IsAggregated;
            }

            return First.AggregationType;
        }

        public string GetDebuggerDisplay()
        {
            return $"{Operator} ({First.GetDebuggerDisplay()})";
        }

        public string BuildFromString(BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias = null)
        {
            var first = First.BuildFromString(state, sqlFragmentBuilder, wrappedQueryAlias);

            return AddUnaryCondition(
                sqlFragmentBuilder,
                first, 
                Operator,
                First.HasOneItemOnly);
        }

        static string AddUnaryCondition(ISqlSyntax sqlFragmentBuilder, string input, UnarySqlOperator condition, bool hasOneItemOnly)
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
                    throw new InvalidOperationException($"Cannot build sql expression for expression type {condition}.");
            }

            string Func(string functionName) => $"{functionName}({input})";
        }
    }
}