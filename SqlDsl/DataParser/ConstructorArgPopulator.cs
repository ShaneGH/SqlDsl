using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.Utils;

namespace SqlDsl.DataParser
{
    public class ConstructorArgPopulator
    {
        public bool[] Booleans;
        public byte[] Bytes;
        public char[] Chars;
        public DateTime[] DateTimes;
        public decimal[] Decimals;
        public double[] Doubles;
        public float[] Floats;
        public Guid[] Guids;
        public short[] Int16s;
        public int[] Int32s;
        public long[] Int64s;

        // todo: use this as a fallback for types which
        // cannot be parsed by other means
        public object[] ReferenceObjects;

        static readonly Dictionary<Type, TypeWorker> _collectionInitializers = BuildCollectionInitializers();
        readonly Action<IDataRecord, ConstructorArgPopulator> _rowParser;

        public ConstructorArgPopulator(ObjectPropertyGraph propertyGraph)
        {
            var cParams = propertyGraph.ConstructorArgTypes;
            for (int arrayLength = cParams.Length; arrayLength > 0; arrayLength--)
            {
                var i = arrayLength - 1;
                if (_collectionInitializers.TryGetValue(cParams[i], out var setter))
                    setter.InitializeCArg(this, arrayLength);
                else
                    InitializeReferenceObjects(this, arrayLength);
            }

            _rowParser = GetRowParser(propertyGraph.SimpleConstructorArgs);
        }

        public void ParseRow(IDataRecord data) => _rowParser(data, this);

        static ConcurrentDictionary<Tuple<int, int, Type, Type>[], Action<IDataRecord, ConstructorArgPopulator>> _simpleConstructorArgPopulators = new ConcurrentDictionary<Tuple<int, int, Type, Type>[], Action<IDataRecord, ConstructorArgPopulator>>();
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
                if (!_collectionInitializers.TryGetValue(arg.DataCellType, out var setter))
                    throw new NotImplementedException();

                return Expression.Assign(
                    Expression.ArrayIndex(
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

        static Dictionary<Type, TypeWorker> BuildCollectionInitializers()
        {
            return new Dictionary<Type, TypeWorker>
            {
                { typeof(bool), new TypeWorker(InitializeBooleans, nameof(Booleans), nameof(IDataReader.GetBoolean)) },
                { typeof(byte), new TypeWorker(InitializeBytes, nameof(Bytes), nameof(IDataReader.GetByte)) },
                { typeof(char), new TypeWorker(InitializeChars, nameof(Chars), nameof(IDataReader.GetChar)) },
                { typeof(DateTime), new TypeWorker(InitializeDateTimes, nameof(DateTimes), nameof(IDataReader.GetDateTime)) },
                { typeof(decimal), new TypeWorker(InitializeDecimals, nameof(Decimals), nameof(IDataReader.GetDecimal)) },
                { typeof(double), new TypeWorker(InitializeDoubles, nameof(Doubles), nameof(IDataReader.GetDouble)) },
                { typeof(float), new TypeWorker(InitializeFloats, nameof(Floats), nameof(IDataReader.GetFloat)) },
                { typeof(Guid), new TypeWorker(InitializeGuids, nameof(Guids), nameof(IDataReader.GetGuid)) },
                { typeof(short), new TypeWorker(InitializeInt16s, nameof(Int16s), nameof(IDataReader.GetInt16)) },
                { typeof(int), new TypeWorker(InitializeInt32s, nameof(Int32s), nameof(IDataReader.GetInt32)) },
                { typeof(long), new TypeWorker(InitializeInt64s, nameof(Int64s), nameof(IDataReader.GetInt64)) },
                { typeof(object), new TypeWorker(InitializeReferenceObjects, nameof(ReferenceObjects), nameof(IDataReader.GetValue)) }
            };
        }

        static void InitializeBooleans(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.Booleans != null) return;
            onObject.Booleans = new bool[length];
        }

        static void InitializeBytes(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.Bytes != null) return;
            onObject.Bytes = new byte[length];
        }

        static void InitializeChars(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.Chars != null) return;
            onObject.Chars = new char[length];
        }

        static void InitializeDateTimes(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.DateTimes != null) return;
            onObject.DateTimes = new DateTime[length];
        }

        static void InitializeDecimals(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.Decimals != null) return;
            onObject.Decimals = new decimal[length];
        }

        static void InitializeDoubles(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.Doubles != null) return;
            onObject.Doubles = new double[length];
        }

        static void InitializeFloats(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.Floats != null) return;
            onObject.Floats = new float[length];
        }

        static void InitializeGuids(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.Guids != null) return;
            onObject.Guids = new Guid[length];
        }

        static void InitializeInt16s(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.Int16s != null) return;
            onObject.Int16s = new short[length];
        }

        static void InitializeInt32s(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.Int32s != null) return;
            onObject.Int32s = new int[length];
        }

        static void InitializeInt64s(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.Int64s != null) return;
            onObject.Int64s = new long[length];
        }

        static void InitializeReferenceObjects(ConstructorArgPopulator onObject, int length)
        {
            if (onObject.ReferenceObjects != null) return;
            onObject.ReferenceObjects = new object[length];
        }

        private class TypeWorker
        {
            public Action<ConstructorArgPopulator, int> InitializeCArg { get; }
            public string PropertyName { get; }
            public string ParserMethodName { get; }

            public TypeWorker(Action<ConstructorArgPopulator, int> initializeCArg, string propertyName, string parserMethodName)
            {
                InitializeCArg = initializeCArg;
                PropertyName = propertyName;
                ParserMethodName = parserMethodName;
            }
        }
    }
}