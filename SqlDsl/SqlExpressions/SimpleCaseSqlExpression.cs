using System;
using System.Collections.Generic;
using System.Linq;
using SqlDsl.Mapper;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.SqlExpressions
{
    class SimpleCaseSqlExpression<T> : ISqlExpression<T>
    {
        public readonly ISqlExpression<T> Subject;
        public readonly (ISqlExpression<T> when, ISqlExpression<T> then)[] Cases;
        public readonly ISqlExpression<T> Else;

        public SimpleCaseSqlExpression(
            ISqlExpression<T> subject,
            IEnumerable<(ISqlExpression<T> when, ISqlExpression<T> then)> cases,
            ISqlExpression<T> @else)
        {
            Subject = subject ?? throw new ArgumentNullException(nameof(subject));
            Cases = cases?.ToArray() ?? throw new ArgumentNullException(nameof(cases));
            Else = @else ?? throw new ArgumentNullException(nameof(@else));
        }

        public bool HasOneItemOnly => false;

        public T First => Subject.First;

        public AggregationType AggregationType => GetAggregationType();

        public string BuildFromString(BuildMapState state, ISqlSyntax sqlFragmentBuilder, string wrappedQueryAlias = null)
        {
            var subject = Subject.BuildFromString(state, sqlFragmentBuilder, wrappedQueryAlias);
            var cases = Cases.Select(c => (
                when: c.when.BuildFromString(state, sqlFragmentBuilder, wrappedQueryAlias),
                then: c.then.BuildFromString(state, sqlFragmentBuilder, wrappedQueryAlias)));
            var @else = Else.BuildFromString(state, sqlFragmentBuilder, wrappedQueryAlias);

            return $"CASE {subject} {cases.Select(c => $"WHEN {c.when} THEN {c.then}").JoinString(" ")} ELSE {@else} END";
        }

        public ISqlExpression<T> Combine(ISqlExpression<T> x, BinarySqlOperator combiner) =>
            new BinarySqlExpression<T>(this, x, combiner);

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

        public ISqlExpression<U> MapParam<U>(Func<T, U> map)
        {
            return new SimpleCaseSqlExpression<U>(
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