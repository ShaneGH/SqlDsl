using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.Mapper
{
    class SimpleCaseAccumulator<T> : IAccumulator<T>
    {
        public readonly IAccumulator<T> Subject;
        public readonly (IAccumulator<T> when, IAccumulator<T> then)[] Cases;
        public readonly IAccumulator<T> Else;

        public SimpleCaseAccumulator(
            IAccumulator<T> subject,
            IEnumerable<(IAccumulator<T> when, IAccumulator<T> then)> cases,
            IAccumulator<T> @else)
        {
            Subject = subject ?? throw new ArgumentNullException(nameof(subject));
            Cases = cases?.ToArray() ?? throw new ArgumentNullException(nameof(cases));
            Else = @else ?? throw new ArgumentNullException(nameof(@else));
        }

        public bool HasOneItemOnly => false;

        public T First => Subject.First;

        public AggregationType AggregationType => GetAggregationType();

        public IAccumulator<T> Combine(IAccumulator<T> x, BinarySqlOperator combiner) =>
            new BinaryAccumulator<T>(this, x, combiner);

        public IEnumerable<(bool isAggregated, T element)> GetAggregatedEnumerable() => Cases
            .SelectMany(c => new[] { c.when, c.then })
            .Append(Else)
            .Prepend(Subject)
            .SelectMany(x => x.GetAggregatedEnumerable());

        public IEnumerable<T> GetEnumerable() => Cases
            .SelectMany(c => new[] { c.when, c.then })
            .Append(Else)
            .Prepend(Subject)
            .SelectMany(x => x.GetEnumerable());

        public IAccumulator<U> MapParam<U>(Func<T, U> map)
        {
            return new SimpleCaseAccumulator<U>(
                Subject.MapParam(map),
                Cases.Select(c => (c.when.MapParam(map), c.then.MapParam(map))),
                Else.MapParam(map));
        }

        AggregationType GetAggregationType()
        {
            var containsAggregation = Cases
                .SelectMany(c => new[] { c.when.AggregationType, c.then.AggregationType })
                .Append(Else.AggregationType)
                .Prepend(Subject.AggregationType)
                .Any(x => x != AggregationType.NotAggregated);

            return containsAggregation
                ? AggregationType.ContainsAggregatedPart
                : AggregationType.NotAggregated;
        }
    }
}