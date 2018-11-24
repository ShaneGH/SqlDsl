using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.Utils
{
    /// <summary>
    /// Extension methods for IEnumerable
    /// </summary>
    public static class IEnumerableUtils
    {
        /// <summary>
        /// Ensure that the input has been enumerated by testing whether it is a List<>, T[] or HashSet<T>.If not return a List<>.
        /// </summary>
        public static IEnumerable<T> Enumerate<T>(this IEnumerable<T> xs) =>
            xs is List<T> || xs is T[] || xs is HashSet<T> ? xs : xs.ToList();
        
        /// <summary>
        /// Alias for string.Join(...)
        /// </summary>
        public static string JoinString<T>(this IEnumerable<T> xs, string separator) =>
            string.Join(separator, xs);
        
        /// <summary>
        /// Alias for .Where(x => x != null)
        /// </summary>
        public static IEnumerable<T> RemoveNulls<T>(this IEnumerable<T> xs)
            where T : class => xs.Where(x => x != null);
        
        /// <summary>
        /// Alias for .Where(x => x != null)
        /// </summary>
        public static IEnumerable<string> RemoveNullOrEmpty(this IEnumerable<string> xs) =>
            xs.Where(x => !string.IsNullOrEmpty(x));
        
        /// <summary>
        /// If the input is null, return Empty instead
        /// </summary>
        public static IEnumerable<T> OrEmpty<T>(this IEnumerable<T> xs) =>
            xs ?? Enumerable.Empty<T>();
        
        /// <summary>
        /// If x == null, return [], otherwise return [x]
        /// </summary>
        public static IEnumerable<T> ToEnumerable<T>(this T x)
            where T: class => x == null ? Enumerable.Empty<T>() : new [] { x };
        
        /// <summary>
        /// If x == null, return [], otherwise return [x]
        /// </summary>
        public static IEnumerable<T> ToEnumerableStruct<T>(this T x)
            where T: struct => new [] { x };

        /// <summary>
        /// Convert from IEnumerable<(IEnumerable&lt;T>, IEnumerable&lt;U>)> to (IEnumerable&lt;T>, IEnumerable&lt;U>)
        /// </summary>
        public static (IEnumerable<T>, IEnumerable<U>) AggregateTuple2<T, U>(this IEnumerable<(IEnumerable<T>, IEnumerable<U>)> xs)
        {
            var ys = new List<T>();
            var zs = new List<U>();

            foreach (var x in xs)
            {
                ys.AddRange(x.Item1);
                zs.AddRange(x.Item2);
            }

            return (ys.Skip(0), zs.Skip(0));
        }

        /// <summary>
        /// Combine each item in an enumerable with it's index.
        /// </summary>
        public static IEnumerable<(int index, T item)> WithIndex<T>(this IEnumerable<T> items)
        {
            return items.Select(_WithIndex);

            (int index, T item) _WithIndex(T item, int index) => (index, item);
        }
        
        /// <summary>
        /// Return the index of an item, or -1
        /// </summary>
        public static int IndexOf<T>(this IEnumerable<T> xs, T val)
        {
            var i = 0;
            if (val == null)
            {
                foreach (var x in xs)
                {
                    if (x == null)
                        return i;

                    i++;
                }
            }
            else
            {
                foreach (var x in xs)
                {
                    if (val.Equals(x))
                        return i;
                        
                    i++;
                }
            }

            return -1;
        }
    }
}
