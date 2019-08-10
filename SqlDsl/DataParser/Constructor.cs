using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.ObjectBuilders;
using SqlDsl.Utils;

namespace SqlDsl.DataParser
{
    public class Constructor<T> : ConstructorArgPopulator
    {
        readonly Func<ConstructorArgPopulator, T> _build;

        static readonly Dictionary<Type, string> _propertyNames = BuildPropertyNames();

        public Constructor(ObjectPropertyGraph objectPropertyGraph)
            : base (objectPropertyGraph)
        {
            if (!typeof(T).IsAssignableFrom(objectPropertyGraph.ObjectType))
                throw new InvalidOperationException($"{typeof(T)} is not assignable from {objectPropertyGraph.ObjectType}.");

            _build = BuildBuilder(objectPropertyGraph);
        }

        public T Build() => _build(this);

        static Func<ConstructorArgPopulator, T> BuildBuilder(ObjectPropertyGraph objectPropertyGraph)
        {
            var constructor = objectPropertyGraph.ObjectType.GetConstructor(
                objectPropertyGraph.ConstructorArgTypes);

            var args = Expression.Parameter(typeof(ConstructorArgPopulator));
            var parameters = objectPropertyGraph.ConstructorArgTypes
                .Select(BuildParamGetter);

            return Expression
                .Lambda<Func<ConstructorArgPopulator, T>>(
                    Expression.New(constructor, parameters),
                    args)
                .Compile();

            Expression BuildParamGetter(Type type, int index)
            {
                var iEnumerableType = ReflectionUtils.GetIEnumerableType(type);
                if (iEnumerableType != null)
                {
                    
                }

                if (type.IsEnum)
                {
                    var rawValue = BuildParamGetter(
                        Enum.GetUnderlyingType(type), index);

                    return Expression.Convert(rawValue, type);
                }

                if (_propertyNames.TryGetValue(type, out var property))
                {
                    property = nameof(ReferenceObjects);
                }
                
                var accessor = Expression.ArrayIndex(
                    Expression.PropertyOrField(args, property),
                    Expression.Constant(index));
                    
                var (isEnum, tBuilder) = Enumerables.CreateCollectionExpression(type, accessor);
                return isEnum
                    ? tBuilder
                    : accessor;
            }
        }

        static Dictionary<Type, string> BuildPropertyNames()
        {
            return new Dictionary<Type, string>
            {
                { typeof(bool), nameof(Booleans) },
                { typeof(byte), nameof(Bytes) },
                { typeof(char), nameof(Chars) },
                { typeof(DateTime), nameof(DateTimes) },
                { typeof(decimal), nameof(Decimals) },
                { typeof(double), nameof(Doubles) },
                { typeof(float), nameof(Floats) },
                { typeof(Guid), nameof(Guids) },
                { typeof(short), nameof(Int16s) },
                { typeof(int), nameof(Int32s) },
                { typeof(long), nameof(Int64s) },
                { typeof(object), nameof(ReferenceObjects) }
            };
        }
    }
}