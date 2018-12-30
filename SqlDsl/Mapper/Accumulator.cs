using System;
using System.Collections.Generic;
using System.Linq;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    class Accumulators<TElement> : IAccumulator<TElement>
    {
        public readonly IAccumulator<TElement> First;
        public readonly (IAccumulator<TElement>, CombinationType) Next;

        public Accumulators(IAccumulator<TElement> first, (IAccumulator<TElement>, CombinationType) next)
        {
            First = first;
            Next = next;
        }

        public bool HasOneItemOnly => false;

        TElement IAccumulator<TElement>.First => First.First;

        public IAccumulator<TElement> Combine(IAccumulator<TElement> x, CombinationType combiner)
        {
            return new Accumulators<TElement>(this, (x, combiner));
        }

        public IEnumerable<TElement> GetEnumerable1()
        {
            return First
                .GetEnumerable1()
                .Concat(Next.Item1.GetEnumerable1());
        }

        public IAccumulator<T> MapParam<T>(Func<TElement, T> map)
        {
            var first = First.MapParam(map);
            var second = Next.Item1.MapParam(map);

            return first.Combine(second, Next.Item2);
        }
    }

    class Accumulator<TElement>: IAccumulator<TElement>
    {   
        public bool HasOneItemOnly => !Inner.Next.Any();
        
        public TElement First => Inner.First;
        
        public IEnumerable<(TElement element, CombinationType combiner)> Next => Inner.Next;

        readonly Accumulator<TElement, CombinationType> Inner;
        
        public Accumulator(Accumulator<TElement, CombinationType> acc)
        {
            Inner = acc;
        }

        public IEnumerable<TElement> GetEnumerable1()
        {
            return Inner.GetEnumerable1();
        }


        public IAccumulator<T> MapParam<T>(Func<TElement, T> map)
        {
            return new Accumulator<T>(Inner.Map(map));
        }

        public IAccumulator<TElement> Combine(IAccumulator<TElement> x, CombinationType combiner)
        {
            return new Accumulators<TElement>(this, (x, combiner));
        }
    }
}