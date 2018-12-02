using SqlDsl.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.ObjectBuilders
{
    interface IObjectGraphCache
    {
        ReusableObjectGraph GetGraph(ILogger logger);
        void ReleaseGraph(ReusableObjectGraph graph);
    }

    class ObjectGraphCache : IObjectGraphCache
    {
        private readonly List<ReusableObjectGraph> Objects = new List<ReusableObjectGraph>(16);
        
        public ReusableObjectGraph GetGraph(ILogger logger)
        {
            ReusableObjectGraph graph;
            lock (Objects)
            {
                if (Objects.Count == 0)
                    return new ReusableObjectGraph(this, logger);

                // prevent list reshuffle by taking last item
                graph = Objects[Objects.Count - 1];
                Objects.RemoveAt(Objects.Count - 1);
            }

            return graph;
        }

        public void ReleaseGraph(ReusableObjectGraph graph)
        {
            lock (Objects)
            {
                if (Objects.Count < 256)
                    Objects.Add(graph);
            }
        }
    }
}
