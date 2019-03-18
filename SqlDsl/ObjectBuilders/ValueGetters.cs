using SqlDsl.Utils;
using SqlDsl.Utils.EqualityComparers;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// Builds functions which convert from incoming sql data to a necessary type
    /// </summary>
    public static class ValueGetters
    {
        static object TrueObject = true;
        static object FalseObject = false;
        static object[] EmptyObjects = new object[0];
        static ConcurrentDictionary<ValueGetterKey, object> Getters = new ConcurrentDictionary<ValueGetterKey, object>();

        public static Func<IEnumerable<object>, ILogger, T> GetValueGetter<T>(bool allowsMultiple, bool cellTypeIsEnumerable)
        {
            var key = new ValueGetterKey(typeof(T), allowsMultiple, cellTypeIsEnumerable);
            if (Getters.TryGetValue(key, out object valueGetter))
                return (Func<IEnumerable<object>, ILogger, T>)valueGetter;

            var valueGetter2 = BuildValueGetter<T>(allowsMultiple, cellTypeIsEnumerable);
            Getters.GetOrAdd(key, valueGetter2);
            return valueGetter2;
        }

        public static object GetValueGetter(Type objectType, bool allowsMultiple, bool cellTypeIsEnumerable)
        {
            var key = new ValueGetterKey(objectType, allowsMultiple, cellTypeIsEnumerable);
            if (Getters.TryGetValue(key, out object valueGetter))
                return valueGetter;

            return ReflectionUtils
                .GetMethod(() => GetValueGetter<object>(true, true), objectType)
                .Invoke(null, new[]
                { 
                    allowsMultiple ? TrueObject : FalseObject,
                    cellTypeIsEnumerable ? TrueObject : FalseObject
                });
        }

        static Func<IEnumerable<object>, ILogger, T> BuildValueGetter<T>(bool allowsMultiple, bool cellTypeIsEnumerable)
        {
            var objGetter = TypeConvertors.GetConvertor<T>(cellTypeIsEnumerable);
            return allowsMultiple ?
                (Func<IEnumerable<object>, ILogger, T>)MultipleGetter :
                Getter;

            T MultipleGetter(IEnumerable<object> objects, ILogger logger) => objGetter(objects, logger);

            T Getter(IEnumerable<object> objects, ILogger logger) => objGetter(GetOne<T>(objects), logger);
        }

        /// <summary>
        /// If the enumerable contains 0 items, return default.
        /// If the enumerable contains 1 item, return it.
        /// If the enumerable contains more than 1 item, throw an exception
        /// </summary>
        static object GetOne<T>(IEnumerable items, string propertyName = null)
        {
            var enumerator = items.GetEnumerator();
            if (!enumerator.MoveNext())
                return default(T);

            var result = enumerator.Current;
            if (enumerator.MoveNext())
            {
                if (propertyName == null)
                {
                    var type = result?.GetType() ??
                        enumerator.Current?.GetType() ??
                        GetEnumeratedType(enumerator);

                    propertyName = type?.FullName ?? "??";
                }

                throw new InvalidOperationException($"Database has returned more than one item for " +
                    $"{propertyName}, however it only accepts a single item.");   
            }

            return result;
        }

        static Type GetEnumeratedType(IEnumerator values)
        {
            while (values.MoveNext())
            {
                if (values.Current != null)
                    return values.Current.GetType();
            }

            return null;
        }

        class ValueGetterKey : Tuple<Type, bool, bool>
        {
            public Type PropertyType => Item1;
            public bool AllowsMultiple => Item2;
            public bool CellTypeIsEnumerable => Item3;

            public ValueGetterKey(Type propertyType, bool allowsMultiple, bool cellTypeIsEnumerable)
                : base(propertyType, allowsMultiple, cellTypeIsEnumerable) { }
        }
    }
}
