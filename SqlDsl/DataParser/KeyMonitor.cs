using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace SqlDsl.DataParser
{
    public class KeyMonitor
    {
        private readonly IDataRecord _reader;
        object[] _key = null;
        readonly int[] _primaryKeyColumns;
        public bool AtLeastOneRecordFound { get; private set; } = false;

        public KeyMonitor(IDataRecord reader, int[] primaryKeyColumns)
        {             
            _reader = reader;
            _primaryKeyColumns = primaryKeyColumns;
        }

        IEnumerable<object> LoadKey()
        {
            return _primaryKeyColumns
                .Select(x => _reader.GetValue(x));
        }

        public void Reset()
        {
             _key = null;
             AtLeastOneRecordFound = false;
        }
                
        public bool RecordHasChanged()
        {
            if (_key == null)
            {
                var k = LoadKey().ToArray();
                if (k.Any(x => x == null || DBNull.Value.Equals(x)))
                    return false;

                _key = k;
                AtLeastOneRecordFound = true;
                return true;
            }

            bool recordHasChanged = false;
            foreach (var (k, i) in LoadKey().Select((x, i) => (x, i)))
            {
                if (k == null || k == DBNull.Value)
                {
                    _key = null;
                    return true;
                }
                else if (!k.Equals(_key[i]))
                {
                    recordHasChanged = true;
                }

                _key[i] = k;
            }
            
            return recordHasChanged;
        }
                
        public bool NewRecord()
        {
            if (_key == null)
            {
                var k = LoadKey().ToArray();
                if (k.Any(x => x == null || DBNull.Value.Equals(x)))
                    return false;

                _key = k;
                AtLeastOneRecordFound = true;
                return true;
            }

            bool newRecord = false;
            foreach (var (k, i) in LoadKey().Select((x, i) => (x, i)))
            {
                if (k == null || k == DBNull.Value)
                {
                    _key = null;
                    return false;
                }
                else if (!k.Equals(_key[i]))
                {
                    newRecord = true;
                }

                _key[i] = k;
            }
            
            return newRecord;
        }

        // public enum WindowState1
        // {
        //     Open,
        //     Closed
        // }

        public enum WindowState
        {
            ClosedNew,
            ClosedSame,
            OpenNew,
            OpenSame
        }
    }
}