using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using SqlDsl.Utils;
using SqlDsl.Utils.Diagnostics;

namespace SqlDsl.Mapper
{
    [DebuggerDisplay("{GetDebuggerDisplay()}")]
    class BinaryAccumulator<TElement> : IAccumulator<TElement>, IDebuggerDisplay
    {
        public readonly IAccumulator<TElement> First;
        public readonly (IAccumulator<TElement>, BinarySqlOperator) Next;
        public AggregationType AggregationType => GetAggregationType();

        public BinaryAccumulator(IAccumulator<TElement> first, (IAccumulator<TElement>, BinarySqlOperator) next)
        {
            First = first;
            Next = next;
        }

        public bool HasOneItemOnly => false;

        TElement IAccumulator<TElement>.First => First.First;

        public IAccumulator<TElement> Combine(IAccumulator<TElement> x, BinarySqlOperator combiner)
        {
            return new BinaryAccumulator<TElement>(this, (x, combiner));
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

        public IAccumulator<T> MapParam<T>(Func<TElement, T> map)
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