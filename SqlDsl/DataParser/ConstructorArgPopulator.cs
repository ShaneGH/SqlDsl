using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.Utils;
using SqlDsl.Utils.EqualityComparers;

namespace SqlDsl.DataParser
{
    public class ConstructorArgPopulator
    {
        public bool[] BooleanCArgs;
        public byte[] ByteCArgs;
        public char[] CharCArgs;
        public DateTime[] DateTimeCArgs;
        public decimal[] DecimalCArgs;
        public double[] DoubleCArgs;
        public float[] FloatCArgs;
        public Guid[] GuidCArgs;
        public short[] Int16CArgs;
        public int[] Int32CArgs;
        public long[] Int64CArgs;

        // todo: use this as a fallback for types which
        // cannot be parsed by other means
        public object[] ReferenceObjects;

        static readonly Dictionary<Type, CollectionInitializer> _collectionInitializers = BuildCollectionInitializers();
        readonly Action<IDataRecord, ConstructorArgPopulator> _rowParser;

        public ConstructorArgPopulator(ObjectPropertyGraph propertyGraph)
        {
            var cParams = propertyGraph.ConstructorArgTypes;
            for (int arrayLength = cParams.Length; arrayLength > 0; arrayLength--)
            {
                if (_collectionInitializers.TryGetValue(cParams[arrayLength - 1], out var setter))
                    setter.InitializeCArg(this, arrayLength);
                else
                    InitializeReferenceObjects(this, arrayLength);
            }

            _rowParser = GetRowParser(propertyGraph.SimpleConstructorArgs);
        }

        public void ParseRow(IDataRecord data) => _rowParser(data, this);

        static ConcurrentDictionary<Tuple<int, int, Type, Type>[], Action<IDataRecord, ConstructorArgPopulator>> _simpleConstructorArgPopulators = 
            new ConcurrentDictionary<Tuple<int, int, Type, Type>[], Action<IDataRecord, ConstructorArgPopulator>>(ArrayComparer<Tuple<int, int, Type, Type>>.Instance);
        static Action<IDataRecord, ConstructorArgPopulator> GetRowParser(SimpleConstructorArg[] simpleConstructorArgs)
        {
            var key = simpleConstructorArgs
                .Select(x => Tuple.Create(x.Index, x.ArgIndex, x.ResultPropertyType, x.DataCellType))
                .ToArray();

            if (!_simpleConstructorArgPopulators.TryGetValue(key, out Action<IDataRecord, ConstructorArgPopulator> value))
            {
                value = BuildRowParser(simpleConstructorArgs);
                _simpleConstructorArgPopulators.TryAdd(key, value);
            }
            
            return value; 
        }

        static Action<IDataRecord, ConstructorArgPopulator> BuildRowParser(SimpleConstructorArg[] simpleConstructorArgs)
        {
            var dataRecord = Expression.Parameter(typeof(IDataRecord));
            var args = Expression.Parameter(typeof(ConstructorArgPopulator));

            var populators = simpleConstructorArgs.Select(BuildArg);
            return Expression
                .Lambda<Action<IDataRecord, ConstructorArgPopulator>>(
                    Expression.Block(populators),
                    dataRecord,
                    args)
                .Compile();

            Expression BuildArg(SimpleConstructorArg arg)
            {
                var key = arg.DataCellType.IsEnum
                    ? Enum.GetUnderlyingType(arg.DataCellType)
                    : arg.DataCellType;

                if (!_collectionInitializers.TryGetValue(key, out var setter))
                {
                    // TODO: some kind of warning for boxing custom structs
                    setter = _collectionInitializers[typeof(object)];
                }

                return Expression.Assign(
                    Expression.ArrayAccess(
                        Expression.PropertyOrField(
                            args,
                            setter.PropertyName),
                        Expression.Constant(arg.ArgIndex)),
                    Expression.Call(
                        dataRecord,
                        setter.ParserMethodName,
                        CodingConstants.Empty.Type,
                        Expression.Constant(arg.Index)));
            }
        }

        static Dictionary<Type, CollectionInitializer> BuildCollectionInitializers()
        {
            return new Dictionary<Type, CollectionInitializer>
            {
                { typeof(bool), new CollectionInitializer(InitializeBooleans, nameof(BooleanCArgs), nameof(IDataReader.GetBoolean)) },
                { typeof(byte), new CollectionInitializer(InitializeBytes, nameof(ByteCArgs), nameof(IDataReader.GetByte)) },
                { typeof(char), new CollectionInitializer(InitializeChars, nameof(CharCArgs), nameof(IDataReader.GetChar)) },
                { typeof(DateTime), new CollectionInitializer(InitializeDateTimes, nameof(DateTimeCArgs), nameof(IDataReader.GetDateTime)) },
                { typeof(decimal), new CollectionInitializer(InitializeDecimals, nameof(DecimalCArgs), nameof(IDataReader.GetDecimal)) },
                { typeof(double), new CollectionInitializer(InitializeDoubles, nameof(DoubleCArgs), nameof(IDataReader.GetDouble)) },
                { typeof(float), new CollectionInitializer(InitializeFloats, nameof(FloatCArgs), nameof(IDataReader.GetFloat)) },
                { typeof(Guid), new CollectionInitializer(InitializeGuids, nameof(GuidCArgs), nameof(IDataReader.GetGuid)) },
                { typeof(short), new CollectionInitializer(InitializeInt16s, nameof(Int16CArgs), nameof(IDataReader.GetInt16)) },
                { typeof(int), new CollectionInitializer(InitializeInt32s, nameof(Int32CArgs), nameof(IDataReader.GetInt32)) },
                { typeof(long), new CollectionInitializer(InitializeInt64s, nameof(Int64CArgs), nameof(IDataReader.GetInt64)) },
                { typeof(object), new CollectionInitializer(InitializeReferenceObjects, nameof(ReferenceObjects), nameof(IDataReader.GetValue)) }
            };
        }

        static void InitializeBooleans(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.BooleanCArgs != null) return;
            onObject.BooleanCArgs = new bool[length];
        }

        static void InitializeBytes(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.ByteCArgs != null) return;
            onObject.ByteCArgs = new byte[length];
        }

        static void InitializeChars(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.CharCArgs != null) return;
            onObject.CharCArgs = new char[length];
        }

        static void InitializeDateTimes(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.DateTimeCArgs != null) return;
            onObject.DateTimeCArgs = new DateTime[length];
        }

        static void InitializeDecimals(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.DecimalCArgs != null) return;
            onObject.DecimalCArgs = new decimal[length];
        }

        static void InitializeDoubles(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.DoubleCArgs != null) return;
            onObject.DoubleCArgs = new double[length];
        }

        static void InitializeFloats(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.FloatCArgs != null) return;
            onObject.FloatCArgs = new float[length];
        }

        static void InitializeGuids(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.GuidCArgs != null) return;
            onObject.GuidCArgs = new Guid[length];
        }

        static void InitializeInt16s(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.Int16CArgs != null) return;
            onObject.Int16CArgs = new short[length];
        }

        static void InitializeInt32s(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.Int32CArgs != null) return;
            onObject.Int32CArgs = new int[length];
        }

        static void InitializeInt64s(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.Int64CArgs != null) return;
            onObject.Int64CArgs = new long[length];
        }

        static void InitializeReferenceObjects(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.ReferenceObjects != null) return;
            onObject.ReferenceObjects = new object[length];
        }

        private class CollectionInitializer
        {
            public Action<ConstructorArgPopulator, int> InitializeCArg { get; }
            public string PropertyName { get; }
            public string ParserMethodName { get; }

            public CollectionInitializer(Action<ConstructorArgPopulator, int> initializeCArg, string propertyName, string parserMethodName)
            {
                InitializeCArg = initializeCArg;
                PropertyName = propertyName;
                ParserMethodName = parserMethodName;
            }
        }
    }
}