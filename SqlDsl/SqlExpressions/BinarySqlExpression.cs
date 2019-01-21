using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using SqlDsl.Utils;
using SqlDsl.Utils.Diagnostics;

namespace SqlDsl.SqlExpressions
{
    [DebuggerDisplay("{GetDebuggerDisplay()}")]
    class BinarySqlExpression<TElement> : ISqlExpression<TElement>, IDebuggerDisplay
    {
        public readonly ISqlExpression<TElement> First;
        public readonly (ISqlExpression<TElement>, BinarySqlOperator) Next;
        public AggregationType AggregationType => GetAggregationType();

        public BinarySqlExpression(ISqlExpression<TElement> first, ISqlExpression<TElement> next, BinarySqlOperator combiner)
        {
            First = first;
            Next = (next, combiner);
        }

        public bool HasOneItemOnly => false;

        TElement ISqlExpression<TElement>.First => First.First;

        public ISqlExpression<TElement> Combine(ISqlExpression<TElement> x, BinarySqlOperator combiner)
        {
            return new BinarySqlExpression<TElement>(this, x, combiner);
        }

        public IEnumerable<TElement> GetEnumerable()
        {
            return First
                .GetEnumerable()
                .Concat(Next.Item1.GetEnumerable());
        }

        public IEnumerable<(bool isAggregated, TElement element)> GetAggregatedEnumerable()
        {
            bool isAggregated = AggregationType == AggregationType.IsAggregated;
            
            return First
                .GetAggregatedEnumerable()
                .Concat(Next.Item1.GetAggregatedEnumerable())
                .Select(x => (x.isAggregated || isAggregated, x.element));
        }

        public ISqlExpression<T> MapParam<T>(Func<TElement, T> map)
        {
            var first = First.MapParam(map);
            var second = Next.Item1.MapParam(map);

            return first.Combine(second, Next.Item2);
        }

        public AggregationType GetAggregationType()
        {
            if (First.AggregationType == AggregationType.NotAggregated &&
                Next.Item1.AggregationType == AggregationType.NotAggregated)
                return AggregationType.NotAggregated;

            return AggregationType.ContainsAggregatedPart;
        }

        public string GetDebuggerDisplay()
        {
            return $"({First.GetDebuggerDisplay()} {Next.Item2.ToString()} {Next.Item1.GetDebuggerDisplay()})";
        }
    }
}