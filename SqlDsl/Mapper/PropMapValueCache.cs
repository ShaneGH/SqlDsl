using System;
using SqlDsl.Utils.ObjectCaches;

namespace SqlDsl.Mapper
{
    internal class PropMapValueCache<T> : ObjectCache<PropMapValue<T>>, IPropMapValueCache
    {
        protected override LogMessages? LogMessageType => LogMessages.CreatedPropMapValueAllocation;

        public PropMapValueCache(ILogger logger)
            : base(logger)
        {
        }

        protected override PropMapValue<T> BuildObject()
        {
            return new PropMapValue<T>(this);
        }

        object IPropMapValueCache.ReleaseOrCreateItem() => base.ReleseOrCreateItem();
    }

    public interface IPropMapValueCache
    {
        object ReleaseOrCreateItem();
    }
}