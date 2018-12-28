using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.SqlBuilders;

namespace SqlDsl.Utils
{
    public class Accumulator<TElement, TCombiner>
    {
        public readonly TElement First;
        public readonly IEnumerable<(TElement element, TCombiner combiner)> Next;
        
        public Accumulator(
            TElement first, 
            IEnumerable<(TElement element, TCombiner combiner)> next = null)
        {
            First = first;
            Next = next.OrEmpty();
        }

        public Accumulator<TElement2, TCombiner> Map<TElement2>(Func<TElement, TElement2> map)
        {
            return new Accumulator<TElement2, TCombiner>(map(First), Next.Select(MapNext));

            (TElement2 element, TCombiner combiner) MapNext((TElement element, TCombiner combiner) input)
            {
                var x = map(input.Item1);
                return (x, input.Item2);
            }
        }

        public IEnumerable<TElement> GetEnumerable1()
        {
            yield return First;

            foreach (var x in Next)
                yield return x.Item1;
        }
    }
}
