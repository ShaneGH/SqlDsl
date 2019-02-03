using SqlDsl.Utils;
using SqlDsl.Utils.ObjectCaches;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.ObjectBuilders
{
    class ObjectGraphCache : ObjectCache<ObjectGraph>
    {
        protected override LogMessages? LogMessageType => LogMessages.CreatedObjectGraphAllocation;

        public ObjectGraphCache(ILogger logger)
            : base(logger)
        {
        }

        protected override ObjectGraph BuildObject()
        {
            return new ObjectGraph(this);
        }
    }
}
