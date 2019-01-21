using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.SqlExpressions
{
    class CaseSqlExpression<T> : ISqlExpression<T>
    {
        public readonly (ISqlExpression<T> when, ISqlExpression<T> then)[] Cases;
        public readonly ISqlExpression<T> Else;

        public CaseSqlExpression(
            IEnumerable<(ISqlExpression<T> when, ISqlExpression<T> then)> cases,
            ISqlExpression<T> @else)
        {
            Cases = cases?.ToArray() ?? throw new ArgumentNullException(nameof(cases));
            Else = @else ?? throw new ArgumentNullException(nameof(@else));
        }

        public bool HasOneItemOnly => Cases.Length == 0 && Else.HasOneItemOnly;

        public T First => Cases.Length == 0 ? Else.First : Cases[0].when.First;

        public AggregationType AggregationType => GetAggregationType();

        public ISqlExpression<T> Combine(ISqlExpression<T> x, BinarySqlOperator combiner) =>
            new BinarySqlExpression<T>(this, x, combiner);

        public IEnumerable<(bool isAggregated, T element)> GetAggregatedEnumerable() => Cases
            .SelectMany(c => new[] { c.when, c.then })
            .Append(Else)
            .SelectMany(x => x.GetAggregatedEnumerable());

        public IEnumerable<T> GetEnumerable() => Cases
            .SelectMany(c => new[] { c.when, c.then })
            .Append(Else)
            .SelectMany(x => x.GetEnumerable());

        public ISqlExpression<U> MapParam<U>(Func<T, U> map)
        {
            return new CaseSqlExpression<U>(
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