using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;

namespace SqlDsl.DataParser
{
    public abstract class TheMiniParser<T> : ITheMoFoBitchParser
    {
        List<T> _results = new List<T>(4);
        private readonly IDataRecord _reader;
        private readonly KeyMonitor _keyMonitor;
        readonly int _colIndex;

        public TheMiniParser(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
        {
            _keyMonitor = new KeyMonitor(reader, primaryKeyColumns);
            _reader = reader;
            _colIndex = colIndex;
        }

        public IEnumerable Flush()
        {
            var results = _results;
            _keyMonitor.Reset();
            _results = new List<T>(4);

            return results;
        }

        protected abstract T ParseValue(IDataRecord reader, int index);

        public bool OnNextRow()
        {
            if (_keyMonitor.RecordHasChanged())
            {
                _results.Add(ParseValue(_reader, _colIndex));
                return true;
            }
            
            return false;
        }
    }

    public static class TheMiniParser
    {
        private static ReadOnlyDictionary<Type, Func<IDataRecord, int, int[], object>> _builders = 
            new ReadOnlyDictionary<Type, Func<IDataRecord, int, int[], object>>(BuildBuilders());

        public static ITheMoFoBitchParser Build(Type forType, IDataRecord reader, int colIndex, int[] primaryKeyColumns)
        {
            if (!_builders.TryGetValue(forType, out var value))
            {
                value = _builders[typeof(object)];
            }

            return (ITheMoFoBitchParser)value(reader, colIndex, primaryKeyColumns);
        }

        private static Dictionary<Type, Func<IDataRecord, int, int[], object>> BuildBuilders()
        {
            return new Dictionary<Type, Func<IDataRecord, int, int[], object>>
            {
                { typeof(bool), (x, y, z) => new TheBoolMiniParserX(x, y, z) },
                { typeof(byte), (x, y, z) => new TheByteMiniParserX(x, y, z) },
                { typeof(char), (x, y, z) => new TheCharMiniParserX(x, y, z) },
                { typeof(DateTime), (x, y, z) => new TheDateTimeMiniParserX(x, y, z) },
                { typeof(decimal), (x, y, z) => new TheDecimalMiniParserX(x, y, z) },
                { typeof(double), (x, y, z) => new TheDoubleMiniParserX(x, y, z) },
                { typeof(float), (x, y, z) => new TheFloatMiniParserX(x, y, z) },
                { typeof(Guid), (x, y, z) => new TheGuidMiniParserX(x, y, z) },
                { typeof(short), (x, y, z) => new TheShortMiniParserX(x, y, z) },
                { typeof(int), (x, y, z) => new TheIntMiniParserX(x, y, z) },
                { typeof(long), (x, y, z) => new TheLongMiniParserX(x, y, z) },
                { typeof(object), (x, y, z) => new TheObjectMiniParserX(x, y, z) }
            };
        }

        class TheBoolMiniParserX : TheMiniParser<bool>
        {
            public TheBoolMiniParserX(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override bool ParseValue(IDataRecord reader, int index) => reader.GetBoolean(index);
        }

        class TheByteMiniParserX : TheMiniParser<byte>
        {
            public TheByteMiniParserX(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override byte ParseValue(IDataRecord reader, int index) => reader.GetByte(index);
        }

        class TheIntMiniParserX : TheMiniParser<int>
        {
            public TheIntMiniParserX(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override int ParseValue(IDataRecord reader, int index) => reader.GetInt32(index);
        }

        class TheLongMiniParserX : TheMiniParser<long>
        {
            public TheLongMiniParserX(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override long ParseValue(IDataRecord reader, int index) => reader.GetInt64(index);
        }

        class TheDoubleMiniParserX : TheMiniParser<Double>
        {
            public TheDoubleMiniParserX(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override Double ParseValue(IDataRecord reader, int index) => reader.GetDouble(index);
        }

        class TheDecimalMiniParserX : TheMiniParser<Decimal>
        {
            public TheDecimalMiniParserX(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override Decimal ParseValue(IDataRecord reader, int index) => reader.GetDecimal(index);
        }

        class TheShortMiniParserX : TheMiniParser<short>
        {
            public TheShortMiniParserX(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override short ParseValue(IDataRecord reader, int index) => reader.GetInt16(index);
        }

        class TheGuidMiniParserX : TheMiniParser<Guid>
        {
            public TheGuidMiniParserX(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override Guid ParseValue(IDataRecord reader, int index) => reader.GetGuid(index);
        }

        class TheDateTimeMiniParserX : TheMiniParser<DateTime>
        {
            public TheDateTimeMiniParserX(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override DateTime ParseValue(IDataRecord reader, int index) => reader.GetDateTime(index);
        }

        class TheCharMiniParserX : TheMiniParser<Char>
        {
            public TheCharMiniParserX(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override Char ParseValue(IDataRecord reader, int index) => reader.GetChar(index);
        }

        class TheFloatMiniParserX : TheMiniParser<float>
        {
            public TheFloatMiniParserX(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override float ParseValue(IDataRecord reader, int index) => reader.GetFloat(index);
        }

        class TheObjectMiniParserX : TheMiniParser<Object>
        {
            public TheObjectMiniParserX(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override Object ParseValue(IDataRecord reader, int index) => reader.GetValue(index);
        }
    }
}