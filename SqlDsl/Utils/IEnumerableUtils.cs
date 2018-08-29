using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

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
        /// If the input is null, return Empty instead
        /// </summary>
        public static IEnumerable<T> OrEmpty<T>(this IEnumerable<T> xs) =>
            xs ?? Enumerable.Empty<T>();
        
        /// <summary>
        /// If x == null, return [], otherwise return [x]
        /// </summary>
        public static IEnumerable<T> ToEnumerable<T>(this T x)
            where T: class => x == null ? Enumerable.Empty<T>() : new [] { x };
    }
}
