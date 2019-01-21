using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.Mapper
{
    class CaseAccumulator<T> : IAccumulator<T>
    {
        public readonly (IAccumulator<T> when, IAccumulator<T> then)[] Cases;
        public readonly IAccumulator<T> Else;

        public CaseAccumulator(
            IEnumerable<(IAccumulator<T> when, IAccumulator<T> then)> cases,
            IAccumulator<T> @else)
        {
            Cases = cases?.ToArray() ?? throw new ArgumentNullException(nameof(cases));
            Else = @else ?? throw new ArgumentNullException(nameof(@else));
        }

        public bool HasOneItemOnly => Cases.Length == 0 && Else.HasOneItemOnly;

        public T First => Cases.Length == 0 ? Else.First : Cases[0].when.First;

        public AggregationType AggregationType => GetAggregationType();

        public IAccumulator<T> Combine(IAccumulator<T> x, BinarySqlOperator combiner) =>
            new BinaryAccumulator<T>(this, x, combiner);

        public IEnumerable<(bool isAggregated, T element)> GetAggregatedEnumerable() => Cases
            .SelectMany(c => new[] { c.when, c.then })
            .Append(Else)
            .SelectMany(x => x.GetAggregatedEnumerable());

        public IEnumerable<T> GetEnumerable() => Cases
            .SelectMany(c => new[] { c.when, c.then })
            .Append(Else)
            .SelectMany(x => x.GetEnumerable());

        public IAccumulator<U> MapParam<U>(Func<T, U> map)
        {
            return new CaseAccumulator<U>(
                Cases.Select(c => (c.when.MapParam(map), c.then.MapParam(map))),
                Else.MapParam(map));
        }

        AggregationType GetAggregationType()
        {
            var containsAggregation = Cases
                .SelectMany(c => new[] { c.when.AggregationType, c.then.AggregationType })
                .Append(Else.AggregationType)
                .Any(x => x != AggregationType.NotAggregated);

            return containsAggregation
                ? AggregationType.ContainsAggregatedPart
                : AggregationType.NotAggregated;
        }
    }
}