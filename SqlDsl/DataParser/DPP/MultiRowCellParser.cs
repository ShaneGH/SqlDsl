using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;

namespace SqlDsl.DataParser.DPP
{
    public static class MultiRowCellParser
    {
        private static ReadOnlyDictionary<Type, Func<IDataRecord, int, int[], object>> _builders = 
            new ReadOnlyDictionary<Type, Func<IDataRecord, int, int[], object>>(BuildBuilders());

        public static IParsingCache Build(Type forType, IDataRecord reader, int colIndex, int[] primaryKeyColumns)
        {
            if (!_builders.TryGetValue(forType, out var value))
            {
                value = _builders[typeof(object)];
            }

            return (IParsingCache)value(reader, colIndex, primaryKeyColumns);
        }

        private static Dictionary<Type, Func<IDataRecord, int, int[], object>> BuildBuilders()
        {
            return new Dictionary<Type, Func<IDataRecord, int, int[], object>>
            {
                { typeof(bool), (x, y, z) => new TheBoolMiniParser(x, y, z) },
                { typeof(byte), (x, y, z) => new TheByteMiniParser(x, y, z) },
                { typeof(char), (x, y, z) => new TheCharMiniParser(x, y, z) },
                { typeof(DateTime), (x, y, z) => new TheDateTimeMiniParser(x, y, z) },
                { typeof(decimal), (x, y, z) => new TheDecimalMiniParser(x, y, z) },
                { typeof(double), (x, y, z) => new TheDoubleMiniParser(x, y, z) },
                { typeof(float), (x, y, z) => new TheFloatMiniParser(x, y, z) },
                { typeof(Guid), (x, y, z) => new TheGuidMiniParser(x, y, z) },
                { typeof(short), (x, y, z) => new TheShortMiniParser(x, y, z) },
                { typeof(int), (x, y, z) => new TheIntMiniParser(x, y, z) },
                { typeof(long), (x, y, z) => new TheLongMiniParser(x, y, z) },
                { typeof(object), (x, y, z) => new TheObjectMiniParser(x, y, z) }
            };
        }

        class TheBoolMiniParser : MultiRowCellParser<bool>
        {
            public TheBoolMiniParser(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override bool ParseValue(IDataRecord reader, int index) => reader.GetBoolean(index);
        }

        class TheByteMiniParser : MultiRowCellParser<byte>
        {
            public TheByteMiniParser(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override byte ParseValue(IDataRecord reader, int index) => reader.GetByte(index);
        }

        class TheIntMiniParser : MultiRowCellParser<int>
        {
            public TheIntMiniParser(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override int ParseValue(IDataRecord reader, int index) => reader.GetInt32(index);
        }

        class TheLongMiniParser : MultiRowCellParser<long>
        {
            public TheLongMiniParser(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override long ParseValue(IDataRecord reader, int index) => reader.GetInt64(index);
        }

        class TheDoubleMiniParser : MultiRowCellParser<Double>
        {
            public TheDoubleMiniParser(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override Double ParseValue(IDataRecord reader, int index) => reader.GetDouble(index);
        }

        class TheDecimalMiniParser : MultiRowCellParser<Decimal>
        {
            public TheDecimalMiniParser(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override Decimal ParseValue(IDataRecord reader, int index) => reader.GetDecimal(index);
        }

        class TheShortMiniParser : MultiRowCellParser<short>
        {
            public TheShortMiniParser(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override short ParseValue(IDataRecord reader, int index) => reader.GetInt16(index);
        }

        class TheGuidMiniParser : MultiRowCellParser<Guid>
        {
            public TheGuidMiniParser(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override Guid ParseValue(IDataRecord reader, int index) => reader.GetGuid(index);
        }

        class TheDateTimeMiniParser : MultiRowCellParser<DateTime>
        {
            public TheDateTimeMiniParser(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override DateTime ParseValue(IDataRecord reader, int index) => reader.GetDateTime(index);
        }

        class TheCharMiniParser : MultiRowCellParser<Char>
        {
            public TheCharMiniParser(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override Char ParseValue(IDataRecord reader, int index) => reader.GetChar(index);
        }

        class TheFloatMiniParser : MultiRowCellParser<float>
        {
            public TheFloatMiniParser(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override float ParseValue(IDataRecord reader, int index) => reader.GetFloat(index);
        }

        class TheObjectMiniParser : MultiRowCellParser<Object>
        {
            // TODO: when is this used? Can I add a warning?
            public TheObjectMiniParser(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
                : base(reader, colIndex, primaryKeyColumns) { }

            protected override Object ParseValue(IDataRecord reader, int index) => reader.GetValue(index);
        }
    }
}