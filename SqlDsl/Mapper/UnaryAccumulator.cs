using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using SqlDsl.Utils;
using SqlDsl.Utils.Diagnostics;

namespace SqlDsl.Mapper
{
    [DebuggerDisplay("{GetDebuggerDisplay()}")]
    class UnaryAccumulator<TElement> : IAccumulator<TElement>,  IDebuggerDisplay
    {
        public readonly IAccumulator<TElement> First;
        public readonly UnarySqlOperator Operator;
        public AggregationType AggregationType => GetAggregationType();

        public UnaryAccumulator(IAccumulator<TElement> first, UnarySqlOperator op)
        {
            First = first;
            Operator = op;
        }

        public bool HasOneItemOnly => First.HasOneItemOnly;

        TElement IAccumulator<TElement>.First => First.First;

        public IAccumulator<TElement> Combine(IAccumulator<TElement> x, BinarySqlOperator combiner)
        {
            return new BinaryAccumulator<TElement>(this, x, combiner);
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

        public IAccumulator<T> MapParam<T>(Func<TElement, T> map)
        {
            var first = First.MapParam(map);
            return new UnaryAccumulator<T>(first, Operator);
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
    }
}