using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// Utils to convert objects from one type to another
    /// </summary>
    static class TypeConvertors
    {
        /// <summary>
        /// Get a function which converts from object -> propertyType. If no function found, falls back to casting
        /// </summary>
        public static object GetConvertor(Type propertyType, bool protectAgainstNull)
        {
            if (propertyType == typeof(int))
                return protectAgainstNull ?
                    (Func<object, int>)ToInt32_NullSafe :
                    (Func<object, int>)Convert.ToInt32;

            if (propertyType == typeof(string))
                return (Func<object, string>)Convert.ToString;

            // fall back to casting
            return GetCaster(propertyType);
        }

        /// <summary>
        /// Get a function which casts from object -> propertyType.
        /// </summary>
        static object GetCaster(Type propertyType)
        {
            var input = Expression.Parameter(typeof(object));
            var body = Expression.Convert(input, propertyType);

            // x => (propertyType)x;
            return Expression.Lambda(body, input).Compile();
        }

        /// <summary>
        /// Get a function which casts from object -> int.
        /// </summary>
        static int ToInt32_NullSafe(object input)
        {
            if (input == null) throw new InvalidOperationException("Cannot cast null to Int32");
            return Convert.ToInt32(input);
        }
    }
}
