using SqlDsl.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.ObjectBuilders
{
    interface IObjectGraphCache
    {
        ObjectGraph GetGraph(ILogger logger);
        void ReleaseGraph(ObjectGraph graph);
    }

    class ObjectGraphCache : IObjectGraphCache
    {
        private readonly List<ObjectGraph> Objects = new List<ObjectGraph>(256);
        
        public ObjectGraph GetGraph(ILogger logger)
        {
            ObjectGraph graph;
            lock (Objects)
            {
                if (Objects.Count == 0)
                    return new ObjectGraph(this, logger);

                // prevent list reshuffle by taking last item
                graph = Objects[Objects.Count - 1];
                Objects.RemoveAt(Objects.Count - 1);
            }

            return graph;
        }

        public void ReleaseGraph(ObjectGraph graph)
        {
            lock (Objects)
            {
                if (Objects.Count < 256)
                    Objects.Add(graph);
            }
        }
    }
}
