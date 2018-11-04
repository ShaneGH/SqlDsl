using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.UnitTests.SqlFlavours
{    
    public class TestDataTable
    {
        public int PrimaryKey;

        public byte Byte;
        public sbyte SByte;
        public bool Bool;
        public byte? Byte_N;
        public sbyte? SByte_N;
        public bool? Bool_N;

        public short Short;
        public int Int;
        public long Long;
        public short? Short_N;
        public int? Int_N;
        public long? Long_N;

        public ushort UShort;
        public uint UInt;
        public ulong ULong;
        public ushort? UShort_N;
        public uint? UInt_N;
        public ulong? ULong_N;

        public float Float;
        public double Double;
        public decimal Decimal;
        public float? Float_N;
        public double? Double_N;
        public decimal? Decimal_N;
                
        public char Char;
        public DateTime DateTime;
        public string String;
        public char? Char_N;
        public DateTime? DateTime_N;

        public Guid Guid;
        public Guid? Guid_N;

        public IEnumerable<char> CharEnumerable;
        public List<char> CharList;
        public char[] CharArray;

        public IEnumerable<byte> ByteEnumerable;
        public List<byte> ByteList;
        public byte[] ByteArray;

        public TestEnum TestEnum;
        public TestEnum? TestEnum_N;

        public object NullableValueTypeAsObject;
        public object ReferenceTypeAsObject;
    }

    public enum TestEnum
    {
        Option1 = 1,
        Option2 = 2
    }

    public class TestDataTables
    {
        public static readonly TestDataTable DataTypeTestNotNulled = new TestDataTable
        {
            PrimaryKey = 123,

            Byte = 1,
            SByte = 2,
            Bool = true,
            Byte_N = 3,
            SByte_N = 4,
            Bool_N = true,

            Short = 6,
            Int = 7,
            Long = 8,
            Short_N = 9,
            Int_N = 10,
            Long_N = 11,

            UShort = 12,
            UInt = 13,
            ULong = 14,
            UShort_N = 15,
            UInt_N = 16,
            ULong_N = 17,

            Float = 1.8F,
            Double = 1.9,
            Decimal = 2.1M,
            Float_N = 2.2F,
            Double_N = 2.3,
            Decimal_N = 2.4M,

            Char = 'a',
            DateTime = new DateTime(2000, 1, 1),
            String = "b",
            Char_N = 'c',
            DateTime_N = new DateTime(2000, 1, 2),

            Guid = new Guid("b5591caa-46bc-4cce-b477-0711090848bf"),
            Guid_N = new Guid("031e35c3-3365-473c-a509-edefaf3f49da"),

            CharArray = new [] { 'd', 'e' },
            CharEnumerable = new [] { 'f', 'g' },
            CharList = new List<char> { 'h', 'i' },

            ByteArray = new [] { (byte)25, (byte)26 },
            ByteEnumerable = new [] { (byte)27, (byte)28 },
            ByteList = new List<byte> { 29, 30 },

            TestEnum = TestEnum.Option1,
            TestEnum_N = TestEnum.Option2,

            NullableValueTypeAsObject = 123,
            ReferenceTypeAsObject = "j"
        };
        
        public static readonly TestDataTable DataTypeTestNulled = new TestDataTable
        {
            PrimaryKey = 234,

            Byte = 1,
            SByte = 2,
            Bool = true,
            Byte_N = null,
            SByte_N = null,
            Bool_N = null,

            Short = 6,
            Int = 7,
            Long = 8,
            Short_N = null,
            Int_N = null,
            Long_N = null,

            UShort = 12,
            UInt = 13,
            ULong = 14,
            UShort_N = null,
            UInt_N = null,
            ULong_N = null,

            Float = 1.8F,
            Double = 1.9,
            Decimal = 2.1M,
            Float_N = null,
            Double_N = null,
            Decimal_N = null,

            Char = 'a',
            DateTime = new DateTime(2000, 1, 1),
            String = null,
            Char_N = null,
            DateTime_N = null,

            Guid = new Guid("b5591caa-46bc-4cce-b477-0711090848bf"),
            Guid_N = null,

            CharArray = null,
            CharEnumerable = null,
            CharList = null,

            ByteArray = null,
            ByteEnumerable = null,
            ByteList = null,

            TestEnum = TestEnum.Option1,
            TestEnum_N = null,

            NullableValueTypeAsObject = 123,
            ReferenceTypeAsObject = "j"
        };
    }
}