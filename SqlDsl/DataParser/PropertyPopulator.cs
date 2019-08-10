// using System;
// using System.Collections.Concurrent;
// using System.Collections.Generic;
// using System.Data;
// using System.Linq;
// using System.Linq.Expressions;
// using SqlDsl.Utils;
// using SqlDsl.Utils.EqualityComparers;

// namespace SqlDsl.DataParser
// {
//     interface IPropertyPopulator
//     {
//         bool[] BooleanProps { get; set; }
//         byte[] ByteProps { get; set; }
//         char[] CharProps { get; set; }
//         DateTime[] DateTimeProps { get; set; }
//         decimal[] DecimalProps { get; set; }
//         double[] DoubleProps { get; set; }
//         float[] FloatProps { get; set; }
//         Guid[] GuidProps { get; set; }
//         short[] Int16Props { get; set; }
//         int[] Int32Props { get; set; }
//         long[] Int64Props { get; set; }
//         object[] ReferenceObjectProps { get; set; }
//     }

//     public class PropertyPopulator<T> : Constructor<T>, IPropertyPopulator
//     {
//         public bool[] BooleanProps { get; set; }
//         public byte[] ByteProps { get; set; }
//         public char[] CharProps { get; set; }
//         public DateTime[] DateTimeProps { get; set; }
//         public decimal[] DecimalProps { get; set; }
//         public double[] DoubleProps { get; set; }
//         public float[] FloatProps { get; set; }
//         public Guid[] GuidProps { get; set; }
//         public short[] Int16Props { get; set; }
//         public int[] Int32Props { get; set; }
//         public long[] Int64Props { get; set; }

//         // todo: use this as a fallback for types which
//         // cannot be parsed by other means
//         public object[] ReferenceObjectProps { get; set; }

//         static readonly Dictionary<Type, CollectionInitializer> _collectionInitializers = BuildCollectionInitializers();
//         readonly Action<IDataRecord, IPropertyPopulator> _rowParser;

//         public PropertyPopulator(ObjectPropertyGraph propertyGraph)
//             : base(propertyGraph)
//         {
//             var cParams = propertyGraph.ConstructorArgTypes;
//             for (int arrayLength = cParams.Length; arrayLength > 0; arrayLength--)
//             {
//                 if (_collectionInitializers.TryGetValue(cParams[arrayLength - 1], out var setter))
//                     setter.InitializeCArg(this, arrayLength);
//                 else
//                     InitializeReferenceObjects(this, arrayLength);
//             }

//             _rowParser = GetRowParser(propertyGraph.SimpleConstructorArgs);
//         }

//         public void ParseRow(IDataRecord data) => _rowParser(data, this);

//         static ConcurrentDictionary<Tuple<int, int, Type, Type>[], Action<IDataRecord, IPropertyPopulator>> _simplePropertyPopulators = 
//             new ConcurrentDictionary<Tuple<int, int, Type, Type>[], Action<IDataRecord, IPropertyPopulator>>(ArrayComparer<Tuple<int, int, Type, Type>>.Instance);
//         static Action<IDataRecord, IPropertyPopulator> GetRowParser(SimpleConstructorArg[] simpleConstructorArgs)
//         {
//             var key = simpleConstructorArgs
//                 .Select(x => Tuple.Create(x.Index, x.ArgIndex, x.ResultPropertyType, x.DataCellType))
//                 .ToArray();

//             if (!_simplePropertyPopulators.TryGetValue(key, out Action<IDataRecord, IPropertyPopulator> value))
//             {
//                 value = BuildRowParser(simpleConstructorArgs);
//                 _simplePropertyPopulators.TryAdd(key, value);
//             }
            
//             return value; 
//         }

//         static Action<IDataRecord, IPropertyPopulator> BuildRowParser(SimpleConstructorArg[] simpleConstructorArgs)
//         {
//             var dataRecord = Expression.Parameter(typeof(IDataRecord));
//             var args = Expression.Parameter(typeof(IPropertyPopulator));

//             var populators = simpleConstructorArgs.Select(BuildArg);
//             return Expression
//                 .Lambda<Action<IDataRecord, IPropertyPopulator>>(
//                     Expression.Block(populators),
//                     dataRecord,
//                     args)
//                 .Compile();

//             Expression BuildArg(SimpleConstructorArg arg)
//             {
//                 var key = arg.DataCellType.IsEnum
//                     ? Enum.GetUnderlyingType(arg.DataCellType)
//                     : arg.DataCellType;

//                 if (!_collectionInitializers.TryGetValue(key, out var setter))
//                 {
//                     // TODO: some kind of warning for boxing custom structs
//                     setter = _collectionInitializers[typeof(object)];
//                 }

//                 return Expression.Assign(
//                     Expression.ArrayAccess(
//                         Expression.PropertyOrField(
//                             args,
//                             setter.PropertyName),
//                         Expression.Constant(arg.ArgIndex)),
//                     Expression.Call(
//                         dataRecord,
//                         setter.ParserMethodName,
//                         CodingConstants.Empty.Type,
//                         Expression.Constant(arg.Index)));
//             }
//         }

//         static Dictionary<Type, CollectionInitializer> BuildCollectionInitializers()
//         {
//             return new Dictionary<Type, CollectionInitializer>
//             {
//                 { typeof(bool), new CollectionInitializer(InitializeBooleans, nameof(BooleanProps), nameof(IDataReader.GetBoolean)) },
//                 { typeof(byte), new CollectionInitializer(InitializeBytes, nameof(ByteProps), nameof(IDataReader.GetByte)) },
//                 { typeof(char), new CollectionInitializer(InitializeChars, nameof(CharProps), nameof(IDataReader.GetChar)) },
//                 { typeof(DateTime), new CollectionInitializer(InitializeDateTimes, nameof(DateTimeProps), nameof(IDataReader.GetDateTime)) },
//                 { typeof(decimal), new CollectionInitializer(InitializeDecimals, nameof(DecimalProps), nameof(IDataReader.GetDecimal)) },
//                 { typeof(double), new CollectionInitializer(InitializeDoubles, nameof(DoubleProps), nameof(IDataReader.GetDouble)) },
//                 { typeof(float), new CollectionInitializer(InitializeFloats, nameof(FloatProps), nameof(IDataReader.GetFloat)) },
//                 { typeof(Guid), new CollectionInitializer(InitializeGuids, nameof(GuidProps), nameof(IDataReader.GetGuid)) },
//                 { typeof(short), new CollectionInitializer(InitializeInt16s, nameof(Int16Props), nameof(IDataReader.GetInt16)) },
//                 { typeof(int), new CollectionInitializer(InitializeInt32s, nameof(Int32Props), nameof(IDataReader.GetInt32)) },
//                 { typeof(long), new CollectionInitializer(InitializeInt64s, nameof(Int64Props), nameof(IDataReader.GetInt64)) },
//                 { typeof(object), new CollectionInitializer(InitializeReferenceObjects, nameof(ReferenceObjectProps), nameof(IDataReader.GetValue)) }
//             };
//         }

//         static void InitializeBooleans(IPropertyPopulator onObject, int length)
//         {
//             if (onObject.BooleanProps != null) return;
//             onObject.BooleanProps = new bool[length];
//         }

//         static void InitializeBytes(IPropertyPopulator onObject, int length)
//         {
//             if (onObject.ByteProps != null) return;
//             onObject.ByteProps = new byte[length];
//         }

//         static void InitializeChars(IPropertyPopulator onObject, int length)
//         {
//             if (onObject.CharProps != null) return;
//             onObject.CharProps = new char[length];
//         }

//         static void InitializeDateTimes(IPropertyPopulator onObject, int length)
//         {
//             if (onObject.DateTimeProps != null) return;
//             onObject.DateTimeProps = new DateTime[length];
//         }

//         static void InitializeDecimals(IPropertyPopulator onObject, int length)
//         {
//             if (onObject.DecimalProps != null) return;
//             onObject.DecimalProps = new decimal[length];
//         }

//         static void InitializeDoubles(IPropertyPopulator onObject, int length)
//         {
//             if (onObject.DoubleProps != null) return;
//             onObject.DoubleProps = new double[length];
//         }

//         static void InitializeFloats(IPropertyPopulator onObject, int length)
//         {
//             if (onObject.FloatProps != null) return;
//             onObject.FloatProps = new float[length];
//         }

//         static void InitializeGuids(IPropertyPopulator onObject, int length)
//         {
//             if (onObject.GuidProps != null) return;
//             onObject.GuidProps = new Guid[length];
//         }

//         static void InitializeInt16s(IPropertyPopulator onObject, int length)
//         {
//             if (onObject.Int16Props != null) return;
//             onObject.Int16Props = new short[length];
//         }

//         static void InitializeInt32s(IPropertyPopulator onObject, int length)
//         {
//             if (onObject.Int32Props != null) return;
//             onObject.Int32Props = new int[length];
//         }

//         static void InitializeInt64s(IPropertyPopulator onObject, int length)
//         {
//             if (onObject.Int64Props != null) return;
//             onObject.Int64Props = new long[length];
//         }

//         static void InitializeReferenceObjects(IPropertyPopulator onObject, int length)
//         {
//             if (onObject.ReferenceObjectProps != null) return;
//             onObject.ReferenceObjectProps = new object[length];
//         }

//         private class CollectionInitializer
//         {
//             public Action<IPropertyPopulator, int> InitializeCArg { get; }
//             public string PropertyName { get; }
//             public string ParserMethodName { get; }

//             public CollectionInitializer(Action<IPropertyPopulator, int> initializeCArg, string propertyName, string parserMethodName)
//             {
//                 InitializeCArg = initializeCArg;
//                 PropertyName = propertyName;
//                 ParserMethodName = parserMethodName;
//             }
//         }
//     }
// }