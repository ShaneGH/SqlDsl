using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.Utils;

namespace SqlDsl.DataParser.DPP
{
    public class Builder<T> : PropertyPopulator<T>
    {
        static readonly Dictionary<Type, string> _collectionProperties = BuildCollectionProperties();
        static readonly ReadOnlyDictionary<string, Action<PropertyPopulator<T>, int, T>> _propertySetters = 
            new ReadOnlyDictionary<string, Action<PropertyPopulator<T>, int, T>>(BuildSetters());

        public Builder(ObjectPropertyGraph objectPropertyGraph)
            : base (objectPropertyGraph)
        {
        }

        public override T Build()
        {
            var value = base.Build();
            SetValues(value);
            return value;
        }

        private void SetValues(T obj)
        {
            // TODO: tests for all of these cases
            // TODO: tests for data in the wrong type (e.g. int => long, enum => int)
            // TODO: warnings for structs which need to be cast to objs
            // TODO: all of the above but for constructor args
            if (BooleanProps != null)
            {
                for (var i = 0; i < BooleanProps.Count; i++)
                {
                    _propertySetters[BooleanProps[i].Key](this, i, obj);
                }
            }

            if (ByteProps != null)
            {
                for (var i = 0; i < ByteProps.Count; i++)
                {
                    _propertySetters[ByteProps[i].Key](this, i, obj);
                }
            }

            if (CharProps != null)
            {
                for (var i = 0; i < CharProps.Count; i++)
                {
                    _propertySetters[CharProps[i].Key](this, i, obj);
                }
            }

            if (DateTimeProps != null)
            {
                for (var i = 0; i < DateTimeProps.Count; i++)
                {
                    _propertySetters[DateTimeProps[i].Key](this, i, obj);
                }
            }

            if (DecimalProps != null)
            {
                for (var i = 0; i < DecimalProps.Count; i++)
                {
                    _propertySetters[DecimalProps[i].Key](this, i, obj);
                }
            }

            if (DoubleProps != null)
            {
                for (var i = 0; i < DoubleProps.Count; i++)
                {
                    _propertySetters[DoubleProps[i].Key](this, i, obj);
                }
            }

            if (FloatProps != null)
            {
                for (var i = 0; i < FloatProps.Count; i++)
                {
                    _propertySetters[FloatProps[i].Key](this, i, obj);
                }
            }

            if (GuidProps != null)
            {
                for (var i = 0; i < GuidProps.Count; i++)
                {
                    _propertySetters[GuidProps[i].Key](this, i, obj);
                }
            }

            if (Int16Props != null)
            {
                for (var i = 0; i < Int16Props.Count; i++)
                {
                    _propertySetters[Int16Props[i].Key](this, i, obj);
                }
            }

            if (Int32Props != null)
            {
                for (var i = 0; i < Int32Props.Count; i++)
                {
                    _propertySetters[Int32Props[i].Key](this, i, obj);
                }
            }

            if (Int64Props != null)
            {
                for (var i = 0; i < Int64Props.Count; i++)
                {
                    _propertySetters[Int64Props[i].Key](this, i, obj);
                }
            }

            if (ReferenceObjectProps != null)
            {
                for (var i = 0; i < ReferenceObjectProps.Count; i++)
                {
                    _propertySetters[ReferenceObjectProps[i].Key](this, i, obj);
                }
            }
        }

        static Dictionary<string, Action<PropertyPopulator<T>, int, T>> BuildSetters()
        {
            var setters = ReflectionUtils.GetFieldsAndProperties(typeof(T));
            var populator = Expression.Parameter(typeof(PropertyPopulator<T>));
            var index = Expression.Parameter(typeof(int));
            var obj = Expression.Parameter(typeof(T));

            return Build().ToDictionary(x => x.Item1, x => x.Item2);
            
            IEnumerable<(string, Action<PropertyPopulator<T>, int, T>)> Build()
            {
                foreach (var (name, type, readOnly) in setters)
                {
                    if (readOnly)
                        continue;

                    // TODO: this code is repeated a lot
                    var typeKey = type.IsEnum
                        ? Enum.GetUnderlyingType(type)
                        : type;

                    // TODO: is this used for enums?
                    if (!_collectionProperties.TryGetValue(typeKey, out var collectionName))
                        collectionName = nameof(ReferenceObjectProps);

                    // obj.Value = builder.Values[index].Value
                    var output = Expression
                        .Lambda<Action<PropertyPopulator<T>, int, T>>(
                            Expression.Assign(
                                Expression.PropertyOrField(
                                    obj,
                                    name),
                                EnsureCorrectType(
                                    Expression.Property(
                                        Expression.Property(
                                            Expression.PropertyOrField(
                                                populator,
                                                collectionName),
                                            "Item",
                                            index),
                                    nameof(KeyValuePair<int, int>.Value)),
                                    type)),
                            populator, index, obj)
                        .Compile();
                            
                    yield return (name, output);
                }
            }
        }

        static Dictionary<Type, string> BuildCollectionProperties()
        {
            return new Dictionary<Type, string>
            {
                { typeof(bool), nameof(BooleanProps) },
                { typeof(byte), nameof(ByteProps) },
                { typeof(char), nameof(CharProps) },
                { typeof(DateTime), nameof(DateTimeProps) },
                { typeof(decimal), nameof(DecimalProps) },
                { typeof(double), nameof(DoubleProps) },
                { typeof(float), nameof(FloatProps) },
                { typeof(Guid), nameof(GuidProps) },
                { typeof(short), nameof(Int16Props) },
                { typeof(int), nameof(Int32Props) },
                { typeof(long), nameof(Int64Props) },
                { typeof(object), nameof(ReferenceObjectProps) }
            };
        }
    }
}