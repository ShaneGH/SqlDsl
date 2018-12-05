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

        public Accumulator<TElement, TCombiner> Combine(Accumulator<TElement, TCombiner> x, TCombiner combinator)
        {
            if (x == null) return this;

            return new Accumulator<TElement, TCombiner>(
                First, 
                Next
                    .Append((x.First, combinator))
                    .Concat(x.Next));
        }
    }

    public class ClassAccumulator<TElement, TCombiner> : Accumulator<TElement, TCombiner>
        where TCombiner: class
    {   
        public ClassAccumulator(Accumulator<TElement, TCombiner> basis)
            : base(basis.First, basis.Next)
        {
        }

        public IEnumerable<(TElement, TCombiner)> GetEnumerable2()
        {
            yield return (First, null);

            foreach (var x in Next)
                yield return x;
        }
    }

    public class StructAccumulator<TElement, TCombiner> : Accumulator<TElement, TCombiner>
        where TCombiner: struct
    {   
        public StructAccumulator(Accumulator<TElement, TCombiner> basis)
            : base(basis.First, basis.Next)
        {
        }

        public IEnumerable<(TElement, TCombiner?)> GetEnumerable2()
        {
            yield return (First, null);

            foreach (var x in Next)
                yield return x;
        }
    }
}
