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
        public async Task Build_WithInt32Value_RetudnsValue()
        {
            // arrange
            var constructor = await Builder.Build(new Type[] { typeof(int) });
            var subject = constructor(new TestDataReader(33));

            // act
            var intValue = subject.GetInt32(0);

            // assert
            Assert.AreEqual(33, intValue);
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