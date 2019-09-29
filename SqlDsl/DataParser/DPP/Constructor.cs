using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.ObjectBuilders;
using SqlDsl.Utils;
using SqlDsl.Utils.EqualityComparers;

namespace SqlDsl.DataParser.DPP
{
    public class Constructor<T> : ConstructorArgPopulator
    {
        readonly Func<ConstructorArgPopulator, T> _build;
        static readonly Dictionary<Type, string> _collectionProperties = BuildCollectionProperties();
        static readonly ConcurrentDictionary<Type[], Func<ConstructorArgPopulator, T>> _constructorCache = 
            new ConcurrentDictionary<Type[], Func<ConstructorArgPopulator, T>>(ArrayComparer<Type>.Instance);

        public Constructor(ObjectPropertyGraph objectPropertyGraph)
            : base (objectPropertyGraph)
        {
            if (!typeof(T).IsAssignableFrom(objectPropertyGraph.ObjectType))
                throw new InvalidOperationException($"{typeof(T)} is not assignable from {objectPropertyGraph.ObjectType}.");

            _build = GetBuilder(objectPropertyGraph.ConstructorArgTypes);
        }

        public virtual T Build() => _build(this);

        class REALLYBADCACHE : Utils.ObjectCaches.ICache
        {
            public static REALLYBADCACHE Instance = new REALLYBADCACHE();

            public void CacheItem(object obj)
            {
            }
        }

        static Func<ConstructorArgPopulator, T> GetBuilder(Type[] constructorArgTypes)
        {
            if (!_constructorCache.TryGetValue(constructorArgTypes, out var result))
            {
                var key = new Type[constructorArgTypes.Length];
                Array.Copy(constructorArgTypes, key, key.Length);
                result = BuildBuilder(key);
                _constructorCache.TryAdd(key, result);
            }

            return result;
        }

        static Func<ConstructorArgPopulator, T> BuildBuilder(Type[] constructorArgTypes)
        {
            // TODO: ReflectionUtils.IsPropMapValue(typeof(T)) != null can be statically processed
            // TODO: use prop map value cache, also, set both constructors of PropMapValue to "internal"
            Type t;
            if ((t = ReflectionUtils.IsPropMapValue(typeof(T))) != null)
                return _ => (T)Activator.CreateInstance(typeof(Mapper.PropMapValue<>).MakeGenericType(t), new object[]{REALLYBADCACHE.Instance});

            var constructor = typeof(T).GetConstructor(constructorArgTypes);

            var args = Expression.Parameter(typeof(ConstructorArgPopulator));
            var parameters = constructorArgTypes.Select(BuildParamGetter);

            return Expression
                .Lambda<Func<ConstructorArgPopulator, T>>(
                    Expression.New(constructor, parameters),
                    args)
                .Compile();

            Expression BuildParamGetter(Type type, int index)
            {
                if (type.IsEnum)
                {
                    var rawValue = BuildParamGetter(
                        Enum.GetUnderlyingType(type), index);

                    return ReflectionUtils.Convert(rawValue, type);
                }

                if (!_collectionProperties.TryGetValue(type, out var property))
                    property = nameof(ReferenceObjectCArgs);
                
                return EnsureCorrectType(
                    Expression.ArrayIndex(
                        Expression.PropertyOrField(args, property),
                        Expression.Constant(index)),
                    type);
            }
        }

        // TODO: move somewhere else?
        protected static Expression EnsureCorrectType(Expression accessor, Type expectedType)
        {
            if (expectedType == accessor.Type)
                return accessor;

            // TODO: test for enumerable of Genders (PersonClass: { Genders: [] }
            // TODO: test for one to one of Genders: (PersonClass: { Gender: G }
            if (expectedType.IsEnum)
                return ReflectionUtils.Convert(accessor, expectedType);

            var expectedInner = ReflectionUtils.GetIEnumerableType(expectedType);
            if (expectedInner == null)
            {
                return EnsureCorrectType(
                    Expression.Call(
                        GetGetOneMethod(expectedType),
                        accessor,
                        // TODO: second arg
                        Expression.Constant(null, typeof(string))),
                    expectedType);
            }

            // TODO: is this cast dangerous? Does it cast 1 by 1?
            accessor = ReflectionUtils.Convert(accessor, typeof(IEnumerable<object>));
            var funcParam = Expression.Parameter(typeof(object));
            var mapper = Expression
                .Lambda(
                    EnsureCorrectType(funcParam, expectedInner),
                    funcParam);

            accessor = Expression.Call(
                ReflectionUtils.GetMethod<IEnumerable<object>>(
                    xs => xs.Select(x => 1),
                    new [] { typeof(object), expectedInner }),
                accessor, mapper);
            
            var (isColl, tBuilder) = Enumerables.CreateCollectionExpression(expectedType, accessor);
            if (!isColl)
                // should not occur
                throw new InvalidOperationException($"Types {expectedType} and {accessor.Type} are not compatible.");

            return tBuilder;
        }

        private static readonly ConcurrentDictionary<Type, MethodInfo> GetOneMethods = new ConcurrentDictionary<Type, MethodInfo>();

        static MethodInfo GetGetOneMethod(Type forType)
        {
            if (!GetOneMethods.TryGetValue(forType, out var method))
            {
                GetOneMethods.TryAdd(forType, method = ReflectionUtils
                    .GetMethod(() => GetOne<int>(null, null), forType));
            }

            return method;
        }

        /// <summary>
        /// If the enumerable contains 0 items, return default.
        /// If the enumerable contains 1 item, return it.
        /// If the enumerable contains more than 1 item, throw an exception
        /// </summary>
        static TValue GetOne<TValue>(object input, string propertyName = null)
        {
            if (input is TValue)
            {
                if (typeof(TValue).IsValueType)
                    // TODO: investigate if this is a case
                    throw new Exception("###");

                return (TValue)input;
            }

            var items = input as IEnumerable<TValue>;

            // TODO: this has been copy pasted. (but with a generic removed)
            // The original (I think) was part of the old parsers, and should be deleted
            var enumerator = items.GetEnumerator();
            if (!enumerator.MoveNext())
                return default(TValue);

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
            // TODO: this has been copy pasted.
            // The original (I think) was part of the old parsers, and should be deleted
            while (values.MoveNext())
            {
                if (values.Current != null)
                    return values.Current.GetType();
            }

            return null;
        }

        static Dictionary<Type, string> BuildCollectionProperties()
        {
            return new Dictionary<Type, string>
            {
                { typeof(bool), nameof(BooleanCArgs) },
                { typeof(byte), nameof(ByteCArgs) },
                { typeof(char), nameof(CharCArgs) },
                { typeof(DateTime), nameof(DateTimeCArgs) },
                { typeof(decimal), nameof(DecimalCArgs) },
                { typeof(double), nameof(DoubleCArgs) },
                { typeof(float), nameof(FloatCArgs) },
                { typeof(Guid), nameof(GuidCArgs) },
                { typeof(short), nameof(Int16CArgs) },
                { typeof(int), nameof(Int32CArgs) },
                { typeof(long), nameof(Int64CArgs) },
                { typeof(object), nameof(ReferenceObjectCArgs) }
            };
        }
    }
}