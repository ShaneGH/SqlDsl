using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using SqlDsl.Utils;
using SqlDsl.Utils.Diagnostics;

namespace SqlDsl.SqlExpressions
{
    [DebuggerDisplay("{GetDebuggerDisplay()}")]
    class SqlExpression<TElement>: ISqlExpression<TElement>, IDebuggerDisplay
    {   
        public bool HasOneItemOnly => !Inner.Next.Any();
        
        public TElement First => Inner.First;
        
        public IEnumerable<(TElement element, BinarySqlOperator combiner)> Next => Inner.Next;

        public AggregationType AggregationType => AggregationType.NotAggregated;

        readonly Accumulator<TElement, BinarySqlOperator> Inner;
        
        public SqlExpression(Accumulator<TElement, BinarySqlOperator> acc)
        {
            Inner = acc;
        }

        public IEnumerable<TElement> GetEnumerable()
        {
            return Inner.GetEnumerable1();
        }

        public IEnumerable<(bool isAggregated, TElement element)> GetAggregatedEnumerable()
        {
            bool isAggregated = AggregationType == AggregationType.IsAggregated;
            return GetEnumerable().Select(x => (isAggregated, x));
        }

        public ISqlExpression<T> MapParam<T>(Func<TElement, T> map)
        {
            return new SqlExpression<T>(Inner.Map(map));
        }

        public ISqlExpression<TElement> Combine(ISqlExpression<TElement> x, BinarySqlOperator combiner)
        {
            return new BinarySqlExpression<TElement>(this, x, combiner);
        }

        public IEnumerable<T> GetEnumerable<T>(Func<ISqlExpression<TElement>, TElement, T> mapper)
        {
            return GetEnumerable().Select(x => mapper(this, x));
        }

        public string GetDebuggerDisplay()
        {
            var op = new StringBuilder(First.GetDebuggerDisplay());

            foreach (var el in Next)
                op.Append($" {el.combiner.ToString()} {el.element.GetDebuggerDisplay()}");

            return op.ToString();
        }
    }
}