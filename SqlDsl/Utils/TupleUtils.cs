using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.Utils
{
    /// <summary>
    /// Extension methods for IEnumerable
    /// </summary>
    public static class TupleUtils
    {
        /// <summary>
        /// Convert the object into a ValueTuple2
        /// </summary>
        public static (T1, T2) AddT<T1, T2>(this T1 x, T2 y) => (x, y);
        
        /// <summary>
        /// Convert the object into a ValueTuple2
        /// </summary>
        public static (T1, T2, T3) AddT<T1, T2, T3>(this (T1, T2) x, T3 y) => (x.Item1, x.Item2, y);
        
        /// <summary>
        /// Convert the object into a ValueTuple2
        /// </summary>
        public static (T1, T2, T3, T4) AddT<T1, T2, T3, T4>(this (T1, T2, T3) x, T4 y) => (x.Item1, x.Item2, x.Item3, y);
    }
}
