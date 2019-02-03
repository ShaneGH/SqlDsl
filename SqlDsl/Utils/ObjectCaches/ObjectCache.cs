using SqlDsl.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.Utils.ObjectCaches
{
    interface ICache
    {
        void CacheItem(object obj);
    }
    
    interface IReleasableCache<TObject> : ICache
    {
        TObject ReleseItem();
        TObject ReleseOrCreateItem();
    }

    interface IInitable<TInputArgs> : IDisposable
    {
        void Init(TInputArgs args);
    }

    abstract class ObjectCache<TObject> : IReleasableCache<TObject>
        where TObject : class
    {
        private readonly List<TObject> Objects = new List<TObject>(16);

        protected abstract LogMessages? LogMessageType { get; }

        readonly ILogger Logger;

        public ObjectCache(ILogger logger)
        {
            Logger = logger;
        }
        
        public TObject ReleseItem()
        {
            TObject graph;
            lock (Objects)
            {
                if (Objects.Count == 0)
                    return null;

                // prevent list reshuffle by taking last item
                graph = Objects[Objects.Count - 1];
                Objects.RemoveAt(Objects.Count - 1);
            }

            return graph;
        }
        
        public TObject ReleseOrCreateItem()
        {
            var released = ReleseItem();
            if (released != null)
                return released;
            
            if (Logger.CanLogDebug(LogMessageType))
                Logger.LogDebug("Object graph created", LogMessageType);

            return BuildObject();
        }

        protected abstract TObject BuildObject();

        public void CacheItem(object graph)
        {
            var g = graph as TObject;
            if (g == null) return;

            lock (Objects)
            {
                if (Objects.Count < 256)
                    Objects.Add(g);
            }
        }
    }
}
