using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace SqlDsl.DataParser
{
    public class TheMiniParser<T> : ITheMoFoBitchParser
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

        public bool OnNextRow()
        {
            if (_keyMonitor.RecordHasChanged())
            {
                _results.Add((T)_reader.GetValue(_colIndex));
                return true;
            }
            
            return false;
        }
    }
}