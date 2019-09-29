using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace SqlDsl.DataParser.DPP
{
    public abstract class MultiRowCellParser<T> : IParsingCache
    {
        List<T> _results = new List<T>(4);
        private readonly IDataRecord _reader;
        private readonly KeyMonitor _keyMonitor;
        readonly int _colIndex;

        public MultiRowCellParser(IDataRecord reader, int colIndex, int[] primaryKeyColumns)
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
}