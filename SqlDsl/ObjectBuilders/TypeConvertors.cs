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
                { typeof(byte), ForNonNullable(AddDummyLogger(Convert.ToByte)) },
                { typeof(sbyte), ForNonNullable(AddDummyLogger(Convert.ToSByte)) },
                { typeof(bool), ForNonNullable(AddDummyLogger(Convert.ToBoolean)) },
                { typeof(short), ForNonNullable(AddDummyLogger(Convert.ToInt16)) },
                { typeof(int), ForNonNullable(AddDummyLogger(Convert.ToInt32)) },
                { typeof(long), ForNonNullable(AddDummyLogger(Convert.ToInt64)) },
                { typeof(ushort), ForNonNullable(AddDummyLogger(Convert.ToUInt16)) },
                { typeof(uint), ForNonNullable(AddDummyLogger(Convert.ToUInt32)) },
                { typeof(ulong), ForNonNullable(AddDummyLogger(Convert.ToUInt64)) },
                { typeof(float), ForNonNullable(AddDummyLogger(Convert.ToSingle)) },
                { typeof(double), ForNonNullable(AddDummyLogger(Convert.ToDouble)) },
                { typeof(decimal), ForNonNullable(AddDummyLogger(Convert.ToDecimal)) },
                { typeof(char), ForNonNullable(AddDummyLogger(Convert.ToChar)) },
                { typeof(DateTime), ForNonNullable(AddDummyLogger(Convert.ToDateTime)) },
                { typeof(string), ForNullableClass(AddDummyLogger(Convert.ToString)) },
                { typeof(Guid), ForNonNullable(AddDummyLogger(ConvertGuid)) },
            });

        static Func<object, T> RemoveLogger<T>(Func<object, ILogger, T> basedOn)
        {
            return Removed;
            T Removed(object x) => basedOn(x, null);
        }

        static Func<object, ILogger, T> AddDummyLogger<T>(Func<object, T> basedOn)
        {
            return Removed;
            T Removed(object x, ILogger logger) => basedOn(x);
        }

        /// <summary>
        /// Convert a conversion function into something which thrpw exceptions on nulls and DbNulls
        /// </summary>
        static Func<object, ILogger, T> ForNonNullable<T>(Func<object, ILogger, T> basedOn)
            where T: struct
        {
            var err1 = $"Cannot convert from null to {typeof(T)}";
            var err2 = $"Cannot convert from DBNull to {typeof(T)}";

            return Result;

            T Result(object x, ILogger logger)
            {
                if (x == null) throw new InvalidOperationException(err1);
                if (x is DBNull) throw new InvalidOperationException(err2);
                
                return basedOn(x, logger);
            };
        }

        /// <summary>
        /// Convert a conversion function into something which will guard against nulls and DbNulls
        /// </summary>
        static Func<object, ILogger, T?> ForNullable<T>(Func<object, ILogger, T> basedOn)
            where T: struct
        {
            return Result;
            
            T? Result(object x, ILogger logger)
            {
                if (x is DBNull || x == null) return null;
                return basedOn(x, logger);
            };
        }

        /// <summary>
        /// Convert a conversion function into something which will guard against nulls and DbNulls
        /// </summary>
        static Func<object, ILogger, T> ForNullableClass<T>(Func<object, ILogger, T> basedOn)
            where T: class
        {
            return Result;
            
            T Result(object x, ILogger logger)
            {
                if (x is DBNull || x == null) return null;
                return basedOn(x, logger);
            };
        }

        /// <summary>
        /// Get a function which converts from object -> propertyType. If no function found, falls back to casting
        /// </summary>
        public static Func<object, ILogger, T> GetConvertor<T>()
        {
            var propertyType = typeof(T);
            if (Convertors.TryGetValue(propertyType, out object convertor))
                return (Func<object, ILogger, T>)convertor;

            var convertor2 = BuildConvertor<T>();
            Convertors.GetOrAdd(propertyType, convertor2);
            return convertor2;
        }

        /// <summary>
        /// Get a function which converts from object -> propertyType. If no function found, falls back to casting
        /// </summary>
        static Func<object, ILogger, T> BuildConvertor<T>()
        {
            var propertyType = typeof(T);

            var enumeratedType = ReflectionUtils.GetIEnumerableType(propertyType);
            if (enumeratedType != null)
                return (Func<object, ILogger, T>)BuildEnumerableConvertor(propertyType, enumeratedType);

            if (propertyType.IsEnum)
                return (Func<object, ILogger, T>)BuildEnumCaster(propertyType);
                
            if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                return (Func<object, ILogger, T>)ReflectionUtils
                    .GetMethod(
                        () => GetNullableConvertor<int>(),
                        propertyType.GetGenericArguments()[0])
                    .Invoke(null, new object[0]);
            }

            // fall back to casting
            return (x, _) => (T)x;
        }

        static Func<object, ILogger, T?> GetNullableConvertor<T>()
            where T: struct
        {
            var inner = GetConvertor<T>();
            return ForNullable(inner);   
        }

        /// <summary>
        /// Get a function which converts from (object)IEnumerable&lt;object> -> propertyType. If no function found, falls back to casting
        /// </summary>
        static object BuildEnumerableConvertor(Type collectionType, Type enumeratedType)
        {
            return ReflectionUtils
                .GetMethod(
                    () => BuildEnumerableConvertor<IEnumerable<object>, object>(),
                    collectionType,
                    enumeratedType)
                .Invoke(null, new object[0]);
        }

        /// <summary>
        /// Get a function which converts from (object)IEnumerable&lt;object> -> propertyType. If no function found, falls back to casting
        /// </summary>
        public static Func<object, ILogger, TCollection> BuildEnumerableConvertor<TCollection, T>()
            where TCollection : IEnumerable<T>
        {
            var innerConvertor = GetConvertor<T>();

            var createCollectionInput = Expression.Parameter(typeof(IEnumerable<T>));
            var (isCollection, builder) = Enumerables.CreateCollectionExpression(typeof(TCollection), createCollectionInput);
            if (!isCollection)
            {
                throw new InvalidOperationException($"Unable to create collection expressoin for collection: {typeof(TCollection)}");
            }

            var createCollection = Expression
                .Lambda<Func<IEnumerable<T>, TCollection>>(builder, createCollectionInput)
                .Compile();

            return Convert;

            TCollection Convert(object input, ILogger logger)
            {
                if (input is TCollection) return (TCollection)input;

                if (!(input is IEnumerable))
                {
                    var valsType = GetTypeString(input);
                    throw new InvalidOperationException($"Expecting input type {valsType} to be enumerable.");
                }

                if (logger.CanLogWarning())
                {
                    var valsType = GetTypeString(input);
                    logger.LogWarning($"Converting {valsType} to type {typeof(TCollection)}. " + 
                        $"\"This conversion is inefficient. Consider changing the " + 
                        $"data type to {valsType}");

                    /*original message:
                    logger.LogWarning($"Converting {valsType} to type {collectionType} for property " + 
                        $"\"{propertyName}\". This conversion is inefficient. Consider changing the " + 
                        $"data type of \"{propertyName}\" to {valsType}"); */
                }

                var converted = (input as IEnumerable)
                    .Cast<object>()
                    .Select(x => innerConvertor(x, logger));

                return createCollection(converted);
            }
        }

        // TODO: copy pasted from Objects
        static string GetTypeString(object values)
        {
            if (values == null) return "?";

            var valType = values.GetType();
            if (!(values is IEnumerable))
                return valType.Name;

            var enumer = (values as IEnumerable).GetEnumerator();
            enumer.MoveNext();

            return valType.IsArray ?
                (GetTypeString(enumer.Current) + "[]") :
                (valType.Name + "<" + GetTypeString(enumer.Current) + ">");
        }

        /// <summary>
        /// Get a function which casts from object -> enum.
        /// </summary>
        static object BuildEnumCaster(Type enumType)
        {
            var input = Expression.Parameter(typeof(object));
            var logger = Expression.Parameter(typeof(ILogger));
            var body = Expression.Convert(
                Expression.Invoke(
                    Expression.Constant(Convertors[typeof(int)]),
                    input,
                    logger), 
                enumType);

            // x => (enumType)Convertors[typeof(int)](x);
            return Expression.Lambda(body, input, logger).Compile();
        }

        static Guid ConvertGuid(object x)
        {
            if (x is Guid) return (Guid)x;
            if (x is byte[]) return new Guid(x as byte[]);
            if (x is IEnumerable<byte>) return new Guid((x as IEnumerable<byte>).ToArray());
            if (x is string) return new Guid(x as string);

            throw new InvalidOperationException($"Cannot convert type {x.GetType()} to Guid");
        }
    }
}