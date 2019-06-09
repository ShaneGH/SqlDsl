using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;

namespace SqlDsl.UnitTests.Utils
{
    public class EquivelantCollection
    {
        public static EquivelantCollection<T> Create<T>(IEnumerable<T> values) => new EquivelantCollection<T>(values);
    }

    public class EquivelantCollection<T> : IEnumerable<T>
    {
        public IEnumerable<T> Inner { get; }

        public T this[int key] => Inner.ElementAt(key);

        public int Length => Inner.Count();

        public int Count => Inner.Count();

        public EquivelantCollection(IEnumerable<T> inner)
        {
            if (inner == null)
            {
                throw new ArgumentNullException(nameof(inner));
            }

            Inner = inner;
        }

        public override int GetHashCode()
        {
            return 0;
        }

        public override bool Equals(object other)
        {
            if (other is IEnumerable xs)
            {
                try
                {
                    CollectionAssert.AreEquivalent(this, xs);
                    return true;
                }
                catch (AssertionException)
                {
                }
            }

            return false;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return Inner.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return Inner.GetEnumerator();
        }
    }
}