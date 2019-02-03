using System;
using SqlDsl.Utils.ObjectCaches;

namespace SqlDsl.Mapper
{
    internal class PropMapValue<T> : ReusableObject<T>
    {
        public T Value;

        internal PropMapValue(ICache cache)
            : base(cache)
        {
            if (cache is BadCache)
                throw new InvalidOperationException("The constructor with no args is for expression purposes only.");
        }

        internal PropMapValue()
            : this(new BadCache())
        {
        }

        protected override void _Dispose()
        {
            Value = default(T);
        }

        private class BadCache : ICache
        {
            public void CacheItem(object obj)
            {
                throw new NotImplementedException();
            }
        }
    }
}