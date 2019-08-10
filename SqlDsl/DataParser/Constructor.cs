using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.ObjectBuilders;
using SqlDsl.Utils;
using SqlDsl.Utils.EqualityComparers;

namespace SqlDsl.DataParser
{
    public class Constructor<T> : ConstructorArgPopulator
    {
        readonly Func<ConstructorArgPopulator, T> _build;

        static readonly Dictionary<Type, string> _propertyNames = BuildPropertyNames();
        static readonly ConcurrentDictionary<Type[], Func<ConstructorArgPopulator, T>> _constructorCache = 
            new ConcurrentDictionary<Type[], Func<ConstructorArgPopulator, T>>(ArrayComparer<Type>.Instance);

        public Constructor(ObjectPropertyGraph objectPropertyGraph)
            : base (objectPropertyGraph)
        {
            if (!typeof(T).IsAssignableFrom(objectPropertyGraph.ObjectType))
                throw new InvalidOperationException($"{typeof(T)} is not assignable from {objectPropertyGraph.ObjectType}.");

            _build = BuildBuilder(objectPropertyGraph.ConstructorArgTypes);
        }

        public virtual T Build() => _build(this);

        static Func<ConstructorArgPopulator, T> GetBuilder(Type[] constructorArgTypes)
        {
            if (!_constructorCache.TryGetValue(constructorArgTypes, out var result))
            {
                var key = new Type[constructorArgTypes.Length];
                Array.Copy(constructorArgTypes, key, key.Length);
                result = BuildBuilder(constructorArgTypes);
                _constructorCache.TryAdd(key, result);
            }

            return result;
        }

        static Func<ConstructorArgPopulator, T> BuildBuilder(Type[] constructorArgTypes)
        {
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

                if (!_propertyNames.TryGetValue(type, out var property))
                    property = nameof(ReferenceObjects);
                
                return EnsureCorrectType(
                    Expression.ArrayIndex(
                        Expression.PropertyOrField(args, property),
                        Expression.Constant(index)),
                    type);
            }

            Expression EnsureCorrectType(Expression accessor, Type expectedType)
            {
                if (expectedType.IsEnum)
                    return ReflectionUtils.Convert(accessor, expectedType);

                var expectedInner = ReflectionUtils.GetIEnumerableType(expectedType);
                if (expectedInner == null)
                {
                    return expectedType == accessor.Type
                        ? accessor
                        : ReflectionUtils.Convert(accessor, expectedType);
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
        }

        static Dictionary<Type, string> BuildPropertyNames()
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
                { typeof(object), nameof(ReferenceObjects) }
            };
        }
    }
}