using SqlDsl.ObjectBuilders;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.DataParser
{
    /// <summary>
    /// Comparer which ignores the outer array and compares the elements of an array
    /// </summary>
    class ArrayComparer<T> : IEqualityComparer<T[]>
    {
        /// <summary>
        /// The static instance of this comparer
        /// </summary>
        public static readonly ArrayComparer<T> Instance = new ArrayComparer<T>();

        private ArrayComparer(){}
        
        /// <summary>
        /// Compare 2 arrays
        /// </summary>
        public bool Equals(T[] x, T[] y)
        {
            if (x == null)
                return y == null;

            if (y == null)
                return false;

            if (x.Length != y.Length)
                return false;

            for (var i = 0; i < x.Length; i++)
            {
                if (x[i] == null)
                {
                    if (y[i] != null)
                        return false;
                }
                else if (!x[i].Equals(y[i]))
                {
                    return false;
                }    
            }

            return true;
        }

        /// <summary>
        /// Always returns 1, in order to force equality check
        /// </summary>
        public int GetHashCode(T[] obj) => 1;
    }
}
