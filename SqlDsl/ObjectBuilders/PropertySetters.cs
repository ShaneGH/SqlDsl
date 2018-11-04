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
    using Ex = System.Linq.Expressions.Expression;

    /// <summary>
    /// Builds functions which convert from incoming sql data to a necessary type
    /// </summary>
    public static class PropertySetters
    {
        static object TrueObject = true;
        static object FalseObject = false;
        static object[] EmptyObjects = new object[0];
        static ConcurrentDictionary<PropertySetterKey, object> Setters = new ConcurrentDictionary<PropertySetterKey, object>();

        public static PropertySetter<TObject> GetPropertySetter<TObject, TProperty>(string propertyName)
        {
            var key = new PropertySetterKey(typeof(TObject), propertyName);
            if (Setters.TryGetValue(key, out object propertySetter))
                return (PropertySetter<TObject>)propertySetter;

            var propertySetter2 = BuildPropertySetter<TObject, TProperty>(propertyName);
            Setters.GetOrAdd(key, propertySetter2);
            return propertySetter2;
        }

        public static PropertySetter<TObject> GetPropertySetter<TObject>(Type tProperty, string propertyName)
        {
            var key = new PropertySetterKey(typeof(TObject), propertyName);
            if (Setters.TryGetValue(key, out object propertySetter))
                return (PropertySetter<TObject>)propertySetter;

            return (PropertySetter<TObject>)ReflectionUtils
                .GetMethod(() => GetPropertySetter<object, object>(""), typeof(TObject), tProperty)
                .Invoke(null, new object[] { propertyName });
        }

        public static object GetPropertySetter(Type tObject, Type tProperty, string propertyName)
        {
            var key = new PropertySetterKey(tObject, propertyName);
            if (Setters.TryGetValue(key, out object propertySetter))
                return propertySetter;

            return ReflectionUtils
                .GetMethod(() => GetPropertySetter<object, object>(""), tObject, tProperty)
                .Invoke(null, new object[] { propertyName });
        }

        static PropertySetter<TObject> BuildPropertySetter<TObject, TProperty>(string propertyName)
        {
            var enumCount = ReflectionUtils.CountEnumerables(typeof(TProperty));
            if (enumCount > 0)
            {
                var singleGetter = ValueGetters.GetValueGetter<TProperty>(true, false);
                var singleSetter = BuildPropertySetter<TObject, TProperty>(propertyName, singleGetter);
                
                var enumerableGetter = ValueGetters.GetValueGetter<TProperty>(enumCount > 1, true);
                var enumerableSetter = BuildPropertySetter<TObject, TProperty>(propertyName, enumerableGetter);

                return new PropertySetter<TObject>(singleSetter, enumerableSetter);
            }
            else
            {
                var getter = ValueGetters.GetValueGetter<TProperty>(false, false);
                var setter = BuildPropertySetter<TObject, TProperty>(propertyName, getter);

                return new PropertySetter<TObject>(setter);
            }
        }

        static Action<TObject, IEnumerable<object>, ILogger> BuildPropertySetter<TObject, TProperty>(string propertyName, Func<IEnumerable<object>, ILogger, TProperty> getter)
        {
            var obj = Ex.Parameter(typeof(TObject));
            var values = Ex.Parameter(typeof(IEnumerable<object>));
            var logger = Ex.Parameter(typeof(ILogger));

            return Ex
                .Lambda<Action<TObject, IEnumerable<object>, ILogger>>(
                    Ex.Assign(
                        Ex.PropertyOrField(
                            obj,
                            propertyName),
                        Ex.Invoke(
                            Ex.Constant(getter),
                            values,
                            logger)),
                    obj,
                    values,
                    logger)
                .Compile();
        }

        class PropertySetterKey : Tuple<Type, string>
        {
            public Type ObjectType => Item1;
            public string FieldOrPropertyName => Item2;

            public PropertySetterKey(Type objectType, string fieldOrPropertyName)
                : base(objectType, fieldOrPropertyName) { }
        }
    }
}
