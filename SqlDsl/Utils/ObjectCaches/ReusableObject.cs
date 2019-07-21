using System;

namespace SqlDsl.Utils.ObjectCaches
{
    /// <summary>
    /// Object which can be re-used via Dispose and Init methods
    /// </summary>
    public abstract class ReusableObject : IDisposable
    {
        /// <summary>
        /// The cache that this object belongs to. Calling dispose will return this object to it's cache
        /// </summary>
        readonly ICache Cache;

        internal ReusableObject(ICache cache)
        {
            Cache = cache ?? throw new ArgumentNullException(nameof(cache));
        }

        protected abstract void _Dispose();

        public virtual void Dispose()
        {
            _Dispose();
            Cache.CacheItem(this);
        }
    }
}