using System;
using NUnit.Framework;
using SqlDsl.DataParser.DataRow;
using System.Threading.Tasks;
using System.Data;

namespace SqlDsl.UnitTests.DataParser.DataWor
{
    [TestFixture]
    public class BuilderTests
    {
        [Test]
        public async Task Build_ConstructorSmokeTest()
        {
            // arrange
            var constructor = await Builder.Build(new Type[0]);
            
            // act
            // assert
            var subject = constructor(new TestDataReader());
        }

        [Test]
        public async Task Build_ArgumentOutOfRange_ThrowsCorrectException()
        {
            // arrange
            var constructor = await Builder.Build(new Type[0]);
            
            // act
            var subject = constructor(new TestDataReader());

            // assert
            Assert.Throws<IndexOutOfRangeException>(() => subject.GetInt32(10));
        }

        [Test]
        public async Task Build_InvalidType_ThrowsCorrectException()
        {
            // arrange
            var constructor = await Builder.Build(new Type[] { typeof(int) });
            
            // act
            var subject = constructor(new TestDataReader(44));

            // assert
            Assert.Throws<InvalidOperationException>(() => subject.GetDateTime(0));
        }
        
        [Test, TestCaseSource("DataTypeCases")]
        public async Task Build_WithAllValueTypes_ReturnsCorrectValues(TestCase testCase)
        {
            // arrange
            var constructor = await Builder.Build(new Type[] { testCase.DataType });
            var subject = constructor(new TestDataReader(testCase.Value));

            // act
            var parseValue = testCase.Invoke(subject, 0);

            // assert
            Assert.AreEqual(testCase.Value, parseValue);
        }
        
        [Test, TestCaseSource("DataTypeCases")]
        public async Task ValueIsEqual_WithSameValues_ReturnsTrue(TestCase testCase)
        {
            // arrange
            var constructor = await Builder.Build(new Type[] { testCase.DataType });
            var val1 = constructor(new TestDataReader(testCase.Value));
            var val2 = constructor(new TestDataReader(testCase.Value));

            // act
            var result = val1.ValueIsEqual(val2, 0);

            // assert
            Assert.True(result);
        }
        
        [Test, TestCaseSource("DataTypeCases")]
        public async Task ValueIsEqual_WithDifferentValues_ReturnsFalse(TestCase testCase)
        {
            // arrange
            var constructor = await Builder.Build(new Type[] { testCase.DataType });
            var val1 = constructor(new TestDataReader(testCase.Value));
            var val2 = constructor(new TestDataReader(testCase.AnotherValue));

            // act
            var result = val1.ValueIsEqual(val2, 0);

            // assert
            Assert.False(result, testCase.DataType.Name);
        }
        
        [Test]
        public async Task ValueIsEqual_WithDifferentDataRowTypes_ReturnsFalse()
        {
            // arrange
            var constructor1 = await Builder.Build(new Type[] { typeof(int) });
            var constructor2 = await Builder.Build(new Type[] { typeof(int), typeof(long) });
            var val1 = constructor1(new TestDataReader(1));
            var val2 = constructor2(new TestDataReader(1, 4L));

            // act
            var result = val1.ValueIsEqual(val2, 0);

            // assert
            Assert.False(result);
        }
        
        [Test]
        public async Task ValueIsEqual_WithDifferentDataRowTypes2_ReturnsFalse()
        {
            // arrange
            var constructor1 = await Builder.Build(new Type[] { typeof(int) });
            var constructor2 = await Builder.Build(new Type[] { typeof(long) });
            var val1 = constructor1(new TestDataReader(1));
            var val2 = constructor2(new TestDataReader(1L));

            // act
            var result = val1.ValueIsEqual(val2, 0);

            // assert
            Assert.False(result);
        }

        public static TestCase[] DataTypeCases = new TestCase[]
        {
            new TestCase(typeof(int), 33, 44, (x, y) => x.GetInt32(y)),
            new TestCase(typeof(long), 33L, 44L, (x, y) => x.GetInt64(y)),
            new TestCase(typeof(bool), true, false, (x, y) => x.GetBoolean(y)),
            new TestCase(typeof(byte), (byte)33, (byte)44, (x, y) => x.GetByte(y)),
            new TestCase(typeof(char), 'x', 'y', (x, y) => x.GetChar(y)),
            new TestCase(typeof(DateTime), DateTime.Now, DateTime.Now.AddDays(1), (x, y) => x.GetDateTime(y)),
            new TestCase(typeof(decimal), 33M, 44M, (x, y) => x.GetDecimal(y)),
            new TestCase(typeof(double), 33D, 44D, (x, y) => x.GetDouble(y)),
            new TestCase(typeof(float), 33F, 44F, (x, y) => x.GetFloat(y)),
            new TestCase(typeof(Guid), Guid.NewGuid(), Guid.NewGuid(), (x, y) => x.GetGuid(y)),
            new TestCase(typeof(Int16), (short)33, (short)44, (x, y) => x.GetInt16(y)),
            new TestCase(typeof(string), "hello", "not hello", (x, y) => x.GetValue(y))
        };

        public class TestCase
        {
            public TestCase(Type dataType, object value, object anotherValue, Func<IDataRow, int, object> invoke)
            {
                DataType = dataType;
                Value = value;
                AnotherValue = anotherValue;
                Invoke = invoke;
            }

            public Type DataType { get; }
            public object Value { get; }
            public object AnotherValue { get; }
            public Func<IDataRow, int, object> Invoke { get; }
        }

        private class TestDataReader : IDataReader
        {
            private readonly object[] _values;

            public object this[int i] => throw new NotImplementedException();

            public object this[string name] => throw new NotImplementedException();

            public int Depth => throw new NotImplementedException();

            public bool IsClosed => throw new NotImplementedException();

            public int RecordsAffected => throw new NotImplementedException();

            public int FieldCount => throw new NotImplementedException();

            public TestDataReader(params object[] values)
            {
                _values = values;
            }

            public void Close()
            {
            }

            public void Dispose()
            {
            }

            public bool GetBoolean(int i) => (bool)_values[i];

            public byte GetByte(int i) => (byte)_values[i];

            public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
            {
                throw new NotImplementedException();
            }

            public char GetChar(int i) => (char)_values[i];

            public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
            {
                throw new NotImplementedException();
            }

            public IDataReader GetData(int i)
            {
                throw new NotImplementedException();
            }

            public string GetDataTypeName(int i)
            {
                throw new NotImplementedException();
            }

            public DateTime GetDateTime(int i) => (DateTime)_values[i];

            public decimal GetDecimal(int i) => (decimal)_values[i];

            public double GetDouble(int i) => (double)_values[i];

            public Type GetFieldType(int i)
            {
                throw new NotImplementedException();
            }

            public float GetFloat(int i) => (float)_values[i];
            
            public Guid GetGuid(int i) => (Guid)_values[i];

            public short GetInt16(int i) => (short)_values[i];

            public int GetInt32(int i) => (int)_values[i];

            public long GetInt64(int i) => (long)_values[i];

            public string GetName(int i)
            {
                throw new NotImplementedException();
            }

            public int GetOrdinal(string name)
            {
                throw new NotImplementedException();
            }

            public DataTable GetSchemaTable()
            {
                throw new NotImplementedException();
            }

            public string GetString(int i) => (string)_values[i];

            public object GetValue(int i) => _values[i];

            public int GetValues(object[] values)
            {
                throw new NotImplementedException();
            }

            public bool IsDBNull(int i) => null == _values[i];

            public bool NextResult()
            {
                throw new NotImplementedException();
            }

            public bool Read()
            {
                throw new NotImplementedException();
            }
        }
    }
}