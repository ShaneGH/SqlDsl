using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.Utils;

namespace SqlDsl.ObjectBuilders
{
    /// <summary>
    /// Utils to convert objects from one type to another
    /// </summary>
    static class TypeConvertors
    {
        static readonly ConcurrentDictionary<Type, object> Convertors = new ConcurrentDictionary<Type, object>(
            new Dictionary<Type, object>
            {
                { typeof(byte), ForNonNullable(Convert.ToByte) },
                { typeof(byte?), ForNullable(Convert.ToByte) },
                
                { typeof(sbyte), ForNonNullable(Convert.ToSByte) },
                { typeof(sbyte?), ForNullable(Convert.ToSByte) },
                
                { typeof(bool), ForNonNullable(Convert.ToBoolean) },
                { typeof(bool?), ForNullable(Convert.ToBoolean) },

                { typeof(short), ForNonNullable(Convert.ToInt16) },
                { typeof(int), ForNonNullable(Convert.ToInt32) },
                { typeof(long), ForNonNullable(Convert.ToInt64) },
                { typeof(short?), ForNullable(Convert.ToInt16) },
                { typeof(int?), ForNullable(Convert.ToInt32) },
                { typeof(long?), ForNullable(Convert.ToInt64) },

                { typeof(ushort), ForNonNullable(Convert.ToUInt16) },
                { typeof(uint), ForNonNullable(Convert.ToUInt32) },
                { typeof(ulong), ForNonNullable(Convert.ToUInt64) },
                { typeof(ushort?), ForNullable(Convert.ToUInt16) },
                { typeof(uint?), ForNullable(Convert.ToUInt32) },
                { typeof(ulong?), ForNullable(Convert.ToUInt64) },

                { typeof(float), ForNonNullable(Convert.ToSingle) },
                { typeof(double), ForNonNullable(Convert.ToDouble) },
                { typeof(decimal), ForNonNullable(Convert.ToDecimal) },
                { typeof(float?), ForNullable(Convert.ToSingle) },
                { typeof(double?), ForNullable(Convert.ToDouble) },
                { typeof(decimal?), ForNullable(Convert.ToDecimal) },
                
                { typeof(char), ForNonNullable(Convert.ToChar) },
                { typeof(char?), ForNullable(Convert.ToChar) },
                
                { typeof(DateTime), ForNonNullable(Convert.ToDateTime) },
                { typeof(DateTime?), ForNullable(Convert.ToDateTime) },

                { typeof(string), ForNullableClass(Convert.ToString) },
                
                { typeof(Guid), ForNonNullable(ConvertGuid) },
                { typeof(Guid?), ForNullable(ConvertGuid) },
                
                { typeof(IEnumerable<char>), ForNullableClass(ConvertCharEnumerable) },
                { typeof(List<char>), ForNullableClass(ConvertCharList) },
                { typeof(char[]), ForNullableClass(ConvertCharArray) },
                
                { typeof(IEnumerable<byte>), ForNullableClass(ConvertCharEnumerable) },
                { typeof(List<byte>), ForNullableClass(ConvertByteList) },
                { typeof(byte[]), ForNullableClass(ConvertByteArray) }
            });

        /// <summary>
        /// Convert a conversion function into something which thrpw exceptions on nulls and DbNulls
        /// </summary>
        static Func<object, T> ForNonNullable<T>(Func<object, T> basedOn)
            where T: struct
        {
            var err1 = $"Cannot convert from null to {typeof(T)}";
            var err2 = $"Cannot convert from DBNull to {typeof(T)}";
            return x =>
            {
                if (x is DBNull) throw new InvalidOperationException(err1);
                if (x == null) throw new InvalidOperationException(err2);
                return basedOn(x);
            };
        }

        /// <summary>
        /// Convert a conversion function into something which will guard against nulls and DbNulls
        /// </summary>
        static Func<object, T?> ForNullable<T>(Func<object, T> basedOn)
            where T: struct
        {
            return x =>
            {
                if (x is DBNull || x == null) return null;
                return basedOn(x);
            };
        }

        static readonly MethodInfo _ForNullable = ReflectionUtils
            .GetMethod(() => ForNullable<int>(null))
            .GetGenericMethodDefinition();

        /// <summary>
        /// Convert a conversion function into something which will guard against nulls and DbNulls
        /// </summary>
        static Func<object, T> ForNullableClass<T>(Func<object, T> basedOn)
            where T: class
        {
            return x =>
            {
                if (x is DBNull || x == null) return null;
                return basedOn(x);
            };
        }

        static readonly MethodInfo _ForNullableClass = ReflectionUtils
            .GetMethod(() => ForNullableClass<string>(null))
            .GetGenericMethodDefinition();

        /// <summary>
        /// Get a function which converts from object -> propertyType. If no function found, falls back to casting
        /// </summary>
        public static object GetConvertor(Type propertyType)
        {
            if (Convertors.TryGetValue(propertyType, out object convertor))
                return convertor;

            if (propertyType.IsEnum)
                return Convertors.GetOrAdd(propertyType, BuildEnumCaster(propertyType));
                
            if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var innerType = propertyType.GetGenericArguments()[0];
                var forNullable = innerType.IsValueType ?
                    _ForNullable.MakeGenericMethod(innerType) :
                    _ForNullableClass.MakeGenericMethod(innerType);
                    
                return Convertors.GetOrAdd(
                    propertyType, 
                    forNullable.Invoke(null, new[] { GetConvertor(innerType) }));
            }

            // fall back to casting
            return Convertors.GetOrAdd(propertyType, BuildCaster(propertyType));
        }

        /// <summary>
        /// Get a function which casts from object -> propertyType.
        /// </summary>
        static object BuildCaster(Type propertyType)
        {
            var input = Expression.Parameter(typeof(object));
            var body = Expression.Convert(input, propertyType);

            // x => (propertyType)x;
            return Expression.Lambda(body, input).Compile();
        }

        /// <summary>
        /// Get a function which casts from object -> enum.
        /// </summary>
        static object BuildEnumCaster(Type enumType)
        {
            var input = Expression.Parameter(typeof(object));
            var body = Expression.Convert(
                Expression.Invoke(
                    Expression.Constant(Convertors[typeof(int)]),
                    input), 
                enumType);

            // x => (enumType)Convertors[typeof(int)](x);
            return Expression.Lambda(body, input).Compile();
        }

        static Guid ConvertGuid(object x)
        {
            if (x is Guid) return (Guid)x;
            if (x is byte[]) return new Guid(x as byte[]);
            if (x is IEnumerable<byte>) return new Guid((x as IEnumerable<byte>).ToArray());
            if (x is string) return new Guid(x as string);

            throw new InvalidOperationException($"Cannot convert type {x.GetType()} to Guid");
        }

        static char[] ConvertCharArray(object x) => ConvertCharEnumerable(x).ToArray();

        static List<char> ConvertCharList(object x) => ConvertCharEnumerable(x).ToList();

        static IEnumerable<char> ConvertCharEnumerable(object x) => Convert.ToString(x);


        static byte[] ConvertByteArray(object x) => _ConvertByteEnumerable(x).ToArray();

        static List<byte> ConvertByteList(object x) => _ConvertByteEnumerable(x).ToList();

        static IEnumerable<byte> ConvertByteEnumerable(object x) => _ConvertByteEnumerable(x).Enumerate();

        static IEnumerable<byte> _ConvertByteEnumerable(object x)
        {
            if (x is IEnumerable)
            {
                foreach (var b in x as IEnumerable)
                {
                    yield return Convert.ToByte(b);
                }
            }
        }
    }
}
