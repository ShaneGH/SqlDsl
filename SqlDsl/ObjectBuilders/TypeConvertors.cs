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
    /// Builds functions which convert from an object to a necessary type
    /// </summary>
    static class TypeConvertors
    {
        static readonly ConcurrentDictionary<Tuple<Type, bool>, object> Convertors = new ConcurrentDictionary<Tuple<Type, bool>, object>(
            new Dictionary<Tuple<Type, bool>, object>
            {
                { Tuple.Create(typeof(byte), false), ForNonNullable(AddDummyLogger(Convert.ToByte)) },
                { Tuple.Create(typeof(sbyte), false), ForNonNullable(AddDummyLogger(Convert.ToSByte)) },
                { Tuple.Create(typeof(bool), false), ForNonNullable(AddDummyLogger(Convert.ToBoolean)) },
                { Tuple.Create(typeof(short), false), ForNonNullable(AddDummyLogger(Convert.ToInt16)) },
                { Tuple.Create(typeof(int), false), ForNonNullable(AddDummyLogger(Convert.ToInt32)) },
                { Tuple.Create(typeof(long), false), ForNonNullable(AddDummyLogger(Convert.ToInt64)) },
                { Tuple.Create(typeof(ushort), false), ForNonNullable(AddDummyLogger(Convert.ToUInt16)) },
                { Tuple.Create(typeof(uint), false), ForNonNullable(AddDummyLogger(Convert.ToUInt32)) },
                { Tuple.Create(typeof(ulong), false), ForNonNullable(AddDummyLogger(Convert.ToUInt64)) },
                { Tuple.Create(typeof(float), false), ForNonNullable(AddDummyLogger(Convert.ToSingle)) },
                { Tuple.Create(typeof(double), false), ForNonNullable(AddDummyLogger(Convert.ToDouble)) },
                { Tuple.Create(typeof(decimal), false), ForNonNullable(AddDummyLogger(Convert.ToDecimal)) },
                { Tuple.Create(typeof(char), false), ForNonNullable(AddDummyLogger(Convert.ToChar)) },
                { Tuple.Create(typeof(DateTime), false), ForNonNullable(AddDummyLogger(Convert.ToDateTime)) },
                { Tuple.Create(typeof(string), false), ForNullableClass(AddDummyLogger(Convert.ToString)) },
                { Tuple.Create(typeof(Guid), false), ForNonNullable(AddDummyLogger(ConvertGuid)) },
            });

        static Func<object, ILogger, T> AddDummyLogger<T>(Func<object, T> basedOn)
        {
            return Added;
            T Added(object x, ILogger logger) => basedOn(x);
        }

        /// <summary>
        /// Convert a conversion function into something which throws exceptions on nulls and DbNulls
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
        /// Get a function which converts from object -> ILogger -> propertyType. If no function found, falls back to casting
        /// </summary>
        public static Func<object, ILogger, T> GetConvertor<T>(bool cellTypeIsEnumerable)
        {
            var key = Tuple.Create(typeof(T), cellTypeIsEnumerable);
            if (Convertors.TryGetValue(key, out object convertor))
                return (Func<object, ILogger, T>)convertor;

            var convertor2 = BuildConvertor<T>(cellTypeIsEnumerable);
            Convertors.GetOrAdd(key, convertor2);
            return convertor2;
        }

        /// <summary>
        /// Get a function which converts from object -> ILogger -> propertyType. If no function found, falls back to casting
        /// </summary>
        public static object GetConvertor(Type convertType, bool cellTypeIsEnumerable)
        {
            return ReflectionUtils
                .GetMethod(() => GetConvertor<object>(true), convertType)
                .Invoke(null, new[] { cellTypeIsEnumerable ? TrueObject : FalseObject });
        }

        /// <summary>
        /// Get a function which converts from object -> propertyType. If no function found, falls back to casting
        /// </summary>
        static Func<object, ILogger, T> BuildConvertor<T>(bool cellTypeIsEnumerable)
        {
            var propertyType = typeof(T);

            var enumeratedType = ReflectionUtils.GetIEnumerableType(propertyType);
            if (enumeratedType != null)
            {
                var enumConvertor = (Func<IEnumerable, ILogger, T>)BuildEnumerableConvertor(propertyType, enumeratedType, cellTypeIsEnumerable);
                return BuildConvertor(enumConvertor);
            }

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
            return Cast<T>;
        }

        static T Cast<T>(object x, ILogger _) => (T)x;

        static Func<object, ILogger, T> BuildConvertor<T>(Func<IEnumerable, ILogger, T> enumerableConvertor)
        {
            var t = typeof(T);
            var collectionIsNullable = t.IsInterface || 
                !t.IsValueType ||
                ReflectionUtils.IsNullable(t);

            return Convertor;

            T Convertor(object obj, ILogger logger)
            {
                if ((obj == null || DBNull.Value.Equals(obj)) && collectionIsNullable)
                    return default(T);

                if (!(obj is IEnumerable))
                {
                    var valsType = GetTypeString(obj);
                    throw new InvalidOperationException($"Expecting input type {valsType} to be IEnumerable.");
                }

                return enumerableConvertor(obj as IEnumerable, logger);
            }
        }

        static Func<object, ILogger, T?> GetNullableConvertor<T>()
            where T: struct
        {
            var inner = GetConvertor<T>(false);
            return ForNullable(inner);   
        }

        static object TrueObject = true;
        static object FalseObject = false;
        static object[] EmptyObjects = new object[0];

        /// <summary>
        /// Get a function which converts from IEnumerable -> propertyType. If no function found, falls back to casting
        /// </summary>
        static object BuildEnumerableConvertor(Type collectionType, Type enumeratedType, bool cellTypeIsEnumerable)
        {
            return ReflectionUtils
                .GetMethod(
                    () => BuildEnumerableConvertor<IEnumerable<object>, object>(true),
                    collectionType,
                    enumeratedType)
                .Invoke(null, new [] { cellTypeIsEnumerable ? TrueObject : FalseObject });
        }

        /// <summary>
        /// Get a function which converts from IEnumerable -> propertyType. If no function found, falls back to casting
        /// </summary>
        static Func<IEnumerable, ILogger, TCollection> BuildEnumerableConvertor<TCollection, T>(bool cellTypeIsEnumerable) //bool resultIsCollectionOfCellsCCC, bool cellTypeIsEnumerableCCC)
            where TCollection : IEnumerable<T>
        {
            var innerConvertor = GetConvertor<T>(cellTypeIsEnumerable);

            var createCollectionInput = Expression.Parameter(typeof(IEnumerable<T>));
            var (isCollection, builder) = Enumerables.CreateCollectionExpression(typeof(TCollection), createCollectionInput);
            if (!isCollection)
                throw new InvalidOperationException($"Unable to create collection expressoin for collection: {typeof(TCollection)}");

            var createCollection = Expression
                .Lambda<Func<IEnumerable<T>, TCollection>>(builder, createCollectionInput)
                .Compile();

            var collectionIsNullable = typeof(TCollection).IsInterface || 
                !typeof(TCollection).IsValueType ||
                ReflectionUtils.IsNullable(typeof(TCollection));
            var innerIsEnumerable = ReflectionUtils.GetIEnumerableType(typeof(T)) != null;

            return Convert;

            TCollection Convert(IEnumerable input, ILogger logger)
            {
                if (input is TCollection) return (TCollection)input;

                if (input == null || DBNull.Value.Equals(input))
                {
                    if (collectionIsNullable)
                        return default(TCollection);

                    throw new InvalidOperationException($"Cannot use value of null for type {typeof(TCollection)}");
                }

                if (cellTypeIsEnumerable && !innerIsEnumerable && logger.CanLogWarning(LogMessages.InefficientCastWarning))
                {
                    var valsType = GetTypeString(input);
                    logger.LogWarning($"Converting {valsType} to type {typeof(TCollection)}. " + 
                        $"This conversion is inefficient. Consider changing the " + 
                        $"result data type to {valsType}", LogMessages.InefficientCastWarning);

                    /*original message:
                    $"Converting {valsType} to type {collectionType} for property " + 
                        $"\"{propertyName}\". This conversion is inefficient. Consider changing the " + 
                        $"data type of \"{propertyName}\" to {valsType}" */
                }

                // ToArray ensures that properties of type IEnumerable<T> will
                // have a concrete type (and not an enumerator)
                var converted = ConvertToType(input, logger).ToArray();
                return createCollection(converted);
            }

            IEnumerable<T> ConvertToType(IEnumerable values, ILogger logger)
            {
                foreach (var val in values)
                    yield return innerConvertor(val, logger);
            } 
        }

        /// <summary>
        /// Get a string which accurately represents the data in values.
        /// e.g. if values is string, returns string. If data is object[] where values[0] is string, returns string[]
        /// </summary>
        static string GetTypeString(object values)
        {
            if (values == null) return "?";

            var valType = values.GetType();
            if (!(values is IEnumerable))
                return valType.Name;

            var enumer = (values as IEnumerable).GetEnumerator();
            enumer.MoveNext();

            return valType.IsArray ?
                $"{GetTypeString(enumer.Current)}[]" :
                $"{valType.Name}<{GetTypeString(enumer.Current)}>";
        }

        static readonly object IntConvertor = Convertors[Tuple.Create(typeof(int), false)];

        /// <summary>
        /// Get a function which casts from object -> enum.
        /// </summary>
        static object BuildEnumCaster(Type enumType)
        {
            var input = Expression.Parameter(typeof(object));
            var logger = Expression.Parameter(typeof(ILogger));
            var body = ReflectionUtils.Convert(
                Expression.Invoke(
                    Expression.Constant(IntConvertor),
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