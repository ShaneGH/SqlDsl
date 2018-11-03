using SqlDsl.Utils;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace SqlDsl.Utils.EqualityComparers
{
    class ConstructorKeyComparer : IEqualityComparer<Tuple<Type, Type[]>>
    {
        public static readonly ConstructorKeyComparer Instance = new ConstructorKeyComparer();
 
        private ConstructorKeyComparer() { }

        public bool Equals(Tuple<Type, Type[]> x, Tuple<Type, Type[]> y)
        {
            if (x == null && y == null) return true;
            if (x == null || y == null) return false;

            if (x.Item1 != y.Item1) return false;

            return ArrayComparer<Type>.Instance.Equals(x.Item2, y.Item2);
        }

        public int GetHashCode(Tuple<Type, Type[]> obj)
        {
            return obj.Item1.GetHashCode() ^ ArrayComparer<Type>.Instance.GetHashCode(obj.Item2);
        }
    }
}