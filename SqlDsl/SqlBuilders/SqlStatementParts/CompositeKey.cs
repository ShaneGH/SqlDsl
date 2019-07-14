using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.SqlBuilders.SqlStatementParts
{
    class CompositeKey : ICompositeKey
    {
        readonly IEnumerable<ISelectColumn> _columns;

        // hack. may have stack overflow
        public IQueryTable Table => _columns.Single().IsRowNumberForTable;

        public CompositeKey(ISelectColumn column)
        {
            _columns = new[] { column };
            //Table = column.IsRowNumberForTable ?? throw new Exception($"Column {column.Alias} is not part of a primary key");
        }

        public IEnumerator<ISelectColumn> GetEnumerator() => _columns.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _columns.GetEnumerator();
    }
}