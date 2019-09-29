using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.Utils;
using SqlDsl.Utils.EqualityComparers;

namespace SqlDsl.DataParser.DPP
{
    public class PropertyPopulator<T> : Constructor<T>
    {
        // TODO: 64bit (min) object created for each property value. Should profile
        public List<KeyValuePair<string, bool>> BooleanProps { get; set; }
        public List<KeyValuePair<string, byte>> ByteProps { get; set; }
        public List<KeyValuePair<string, char>> CharProps { get; set; }
        public List<KeyValuePair<string, DateTime>> DateTimeProps { get; set; }
        public List<KeyValuePair<string, decimal>> DecimalProps { get; set; }
        public List<KeyValuePair<string, double>> DoubleProps { get; set; }
        public List<KeyValuePair<string, float>> FloatProps { get; set; }
        public List<KeyValuePair<string, Guid>> GuidProps { get; set; }
        public List<KeyValuePair<string, short>> Int16Props { get; set; }
        public List<KeyValuePair<string, int>> Int32Props { get; set; }
        public List<KeyValuePair<string, long>> Int64Props { get; set; }
        
        // todo: use this as a fallback for types which
        // cannot be parsed by other means
        public readonly List<KeyValuePair<string, object>> ReferenceObjectProps = new List<KeyValuePair<string, object>>(4);

        static readonly Dictionary<Type, CollectionInitializer> _collectionInitializers = BuildCollectionInitializers();
        readonly Action<IDataRecord, PropertyPopulator<T>> _rowParser;

        public PropertyPopulator(ObjectPropertyGraph propertyGraph)
            : base(propertyGraph)
        {
            var simpleProps = propertyGraph.SimpleProps
                // if p == null, this is not a property. (it could be a pk)
                .Where(p => p.ResultPropertyType != null)
                .ToArray();

            // TODO: cache this in ObjectPropertyGraphWrapper
            var propTypes = propertyGraph.ComplexProps
                .Select(cp => cp.Value.ObjectType)
                .Concat(simpleProps
                    .Select(sp => sp.ResultPropertyType));

            foreach (var propType in propTypes)
            {
                // TODO: this code is repeated a lot
                var key = propType.IsEnum
                    ? Enum.GetUnderlyingType(propType)
                    : propType;

                // if false, the value will be appended to object properties
                // which is pre initialized
                if (_collectionInitializers.TryGetValue(key, out var setter))
                    setter.InitializeProperty(this);
            }

            _rowParser = GetRowParser(simpleProps);
        }

        public override void ParseRow(IDataRecord data)
        {
            base.ParseRow(data);
            _rowParser(data, this);
        }

        static ConcurrentDictionary<Tuple<int, string, Type, Type>[], Action<IDataRecord, PropertyPopulator<T>>> _simplePropertyPopulators = 
            new ConcurrentDictionary<Tuple<int, string, Type, Type>[], Action<IDataRecord, PropertyPopulator<T>>>(ArrayComparer<Tuple<int, string, Type, Type>>.Instance);
        
        static Action<IDataRecord, PropertyPopulator<T>> GetRowParser(SimpleProp[] simpleProps)
        {
            var key = simpleProps
                .Select(x => Tuple.Create(x.Index, x.Name, x.ResultPropertyType, x.DataCellType))
                .ToArray();

            if (!_simplePropertyPopulators.TryGetValue(key, out Action<IDataRecord, PropertyPopulator<T>> value))
            {
                value = BuildRowParser(simpleProps);
                _simplePropertyPopulators.TryAdd(key, value);
            }
            
            return value; 
        }

        static Action<IDataRecord, PropertyPopulator<T>> BuildRowParser(SimpleProp[] simpleProps)
        {
            var dataRecord = Expression.Parameter(typeof(IDataRecord));
            var args = Expression.Parameter(typeof(PropertyPopulator<T>));

            var populators = simpleProps.Select(BuildProp);
            return Expression
                .Lambda<Action<IDataRecord, PropertyPopulator<T>>>(
                    Expression.Block(populators),
                    dataRecord,
                    args)
                .Compile();

            // args[cArgIndex] = dataRecord.GetInt32(colIndex);
            Expression BuildProp(SimpleProp prop)
            {
                var key = prop.DataCellType.IsEnum
                    ? Enum.GetUnderlyingType(prop.DataCellType)
                    : prop.DataCellType;

                if (!_collectionInitializers.TryGetValue(key, out var setter))
                {
                    // TODO: some kind of warning for boxing custom structs
                    setter = _collectionInitializers[typeof(object)];
                }

                var collectionType = _collectionInitializers.ContainsKey(key)
                    ? key
                    : typeof(object);

                var kvpArgs = new Type[] { typeof(string), collectionType };
                var kvpConstructor = typeof(KeyValuePair<,>)
                    .MakeGenericType(kvpArgs)
                    .GetConstructor(kvpArgs);

                // this.Int32Props.Add(new KeyValuePair<string, int>("name", dataRecord.GetInt32(index)));
                return Expression.Call(
                    Expression.PropertyOrField(
                        args,
                        setter.PropertyName),
                    "Add",
                    CodingConstants.Empty.Type,
                    Expression.New(
                        kvpConstructor,
                        Expression.Constant(prop.Name),
                        Expression.Call(
                            dataRecord,
                            setter.ParserMethodName,
                            CodingConstants.Empty.Type,
                            Expression.Constant(prop.Index))));
            }
        }

        static Dictionary<Type, CollectionInitializer> BuildCollectionInitializers()
        {
            return new Dictionary<Type, CollectionInitializer>
            {
                { typeof(bool), new CollectionInitializer(InitializeBooleans, nameof(BooleanProps), nameof(IDataReader.GetBoolean)) },
                { typeof(byte), new CollectionInitializer(InitializeBytes, nameof(ByteProps), nameof(IDataReader.GetByte)) },
                { typeof(char), new CollectionInitializer(InitializeChars, nameof(CharProps), nameof(IDataReader.GetChar)) },
                { typeof(DateTime), new CollectionInitializer(InitializeDateTimes, nameof(DateTimeProps), nameof(IDataReader.GetDateTime)) },
                { typeof(decimal), new CollectionInitializer(InitializeDecimals, nameof(DecimalProps), nameof(IDataReader.GetDecimal)) },
                { typeof(double), new CollectionInitializer(InitializeDoubles, nameof(DoubleProps), nameof(IDataReader.GetDouble)) },
                { typeof(float), new CollectionInitializer(InitializeFloats, nameof(FloatProps), nameof(IDataReader.GetFloat)) },
                { typeof(Guid), new CollectionInitializer(InitializeGuids, nameof(GuidProps), nameof(IDataReader.GetGuid)) },
                { typeof(short), new CollectionInitializer(InitializeInt16s, nameof(Int16Props), nameof(IDataReader.GetInt16)) },
                { typeof(int), new CollectionInitializer(InitializeInt32s, nameof(Int32Props), nameof(IDataReader.GetInt32)) },
                { typeof(long), new CollectionInitializer(InitializeInt64s, nameof(Int64Props), nameof(IDataReader.GetInt64)) },
                // this property is pre initialized
                { typeof(object), new CollectionInitializer(_ => {}, nameof(ReferenceObjectProps), nameof(IDataReader.GetValue)) }
            };
        }

        static void InitializeBooleans(PropertyPopulator<T> onObject)
        {
            if (onObject.BooleanProps != null) return;
            onObject.BooleanProps = new List<KeyValuePair<string, bool>>(4);
        }

        static void InitializeBytes(PropertyPopulator<T> onObject)
        {
            if (onObject.ByteProps != null) return;
            onObject.ByteProps = new List<KeyValuePair<string, byte>>(4);
        }

        static void InitializeChars(PropertyPopulator<T> onObject)
        {
            if (onObject.CharProps != null) return;
            onObject.CharProps = new List<KeyValuePair<string, char>>(4);
        }

        static void InitializeDateTimes(PropertyPopulator<T> onObject)
        {
            if (onObject.DateTimeProps != null) return;
            onObject.DateTimeProps = new List<KeyValuePair<string, DateTime>>(4);
        }

        static void InitializeDecimals(PropertyPopulator<T> onObject)
        {
            if (onObject.DecimalProps != null) return;
            onObject.DecimalProps = new List<KeyValuePair<string, decimal>>(4);
        }

        static void InitializeDoubles(PropertyPopulator<T> onObject)
        {
            if (onObject.DoubleProps != null) return;
            onObject.DoubleProps = new List<KeyValuePair<string, double>>(4);
        }

        static void InitializeFloats(PropertyPopulator<T> onObject)
        {
            if (onObject.FloatProps != null) return;
            onObject.FloatProps = new List<KeyValuePair<string, float>>(4);
        }

        static void InitializeGuids(PropertyPopulator<T> onObject)
        {
            if (onObject.GuidProps != null) return;
            onObject.GuidProps = new List<KeyValuePair<string, Guid>>(4);
        }

        static void InitializeInt16s(PropertyPopulator<T> onObject)
        {
            if (onObject.Int16Props != null) return;
            onObject.Int16Props = new List<KeyValuePair<string, short>>(4);
        }

        static void InitializeInt32s(PropertyPopulator<T> onObject)
        {
            if (onObject.Int32Props != null) return;
            onObject.Int32Props = new List<KeyValuePair<string, int>>(4);
        }

        static void InitializeInt64s(PropertyPopulator<T> onObject)
        {
            if (onObject.Int64Props != null) return;
            onObject.Int64Props = new List<KeyValuePair<string, long>>(4);
        }

        private class CollectionInitializer
        {
            public Action<PropertyPopulator<T>> InitializeProperty { get; }
            public string PropertyName { get; }
            public string ParserMethodName { get; }

            public CollectionInitializer(Action<PropertyPopulator<T>> initializeProperty, string propertyName, string parserMethodName)
            {
                InitializeProperty = initializeProperty;
                PropertyName = propertyName;
                ParserMethodName = parserMethodName;
            }
        }
    }
}