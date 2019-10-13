using System;
using System.Collections;
using System.Data;
using System.Data.Common;

namespace SqlDsl.UnitTests.FullPathTests.Environment
{
    class TestReader : DbDataReader, IDisposable
    {
        TestExecutor Executor;
        DbDataReader Reader;
        int Index;

        public TestReader(TestExecutor executor, DbDataReader reader, int index)
        {
            Executor = executor;
            Reader = reader;
            Index = index;
        }

        public override object this[int i] => Reader[i];

        public override object this[string name] => Reader[name];

        public override int Depth => Reader.Depth;

        public override bool IsClosed => Reader.IsClosed;

        public override int RecordsAffected => Reader.RecordsAffected;

        public override int FieldCount => Reader.FieldCount;

        public override bool HasRows => Reader.HasRows;

        public override void Close()
        {
            Reader.Close();
        }

        public new void Dispose()
        {
            Reader.Dispose();
        }

        public override bool GetBoolean(int i)
        {
            return Reader.GetBoolean(i);
        }

        public override byte GetByte(int i)
        {
            return Reader.GetByte(i);
        }

        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return Reader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public override char GetChar(int i)
        {
            return Reader.GetChar(i);
        }

        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return Reader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public override string GetDataTypeName(int i)
        {
            return Reader.GetDataTypeName(i);
        }

        public override DateTime GetDateTime(int i)
        {
            return Reader.GetDateTime(i);
        }

        public override decimal GetDecimal(int i)
        {
            return Reader.GetDecimal(i);
        }

        public override double GetDouble(int i)
        {
            return Reader.GetDouble(i);
        }

        public override IEnumerator GetEnumerator()
        {
            return Reader.GetEnumerator();
        }

        public override Type GetFieldType(int i)
        {
            return Reader.GetFieldType(i);
        }

        public override float GetFloat(int i)
        {
            return Reader.GetFloat(i);
        }

        public override Guid GetGuid(int i)
        {
            return Reader.GetGuid(i);
        }

        public override short GetInt16(int i)
        {
            return Reader.GetInt16(i);
        }

        public override int GetInt32(int i)
        {
            return Reader.GetInt32(i);
        }

        public override long GetInt64(int i)
        {
            return Reader.GetInt64(i);
        }

        public override string GetName(int i)
        {
            return Reader.GetName(i);
        }

        public override int GetOrdinal(string name)
        {
            return Reader.GetOrdinal(name);
        }

        public override DataTable GetSchemaTable()
        {
            return Reader.GetSchemaTable();
        }

        public override string GetString(int i)
        {
            return Reader.GetString(i);
        }

        public override object GetValue(int i)
        {
            return Reader.GetValue(i);
        }

        public override int GetValues(object[] values)
        {
            return Reader.GetValues(values);
        }

        public override bool IsDBNull(int i)
        {
            return Reader.IsDBNull(i);
        }

        public override bool NextResult()
        {
            return Reader.NextResult();
        }

        public override bool Read()
        {
            if (Reader.Read())
            {
                var vals = new object[Reader.FieldCount];
                Reader.GetValues(vals);
                Executor.RecordRow(Index, vals);
                return true;
            }

            return false;
        }
    }
}
