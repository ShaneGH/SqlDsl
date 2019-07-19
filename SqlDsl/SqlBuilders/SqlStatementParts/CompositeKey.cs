using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using SqlDsl.Utils;

namespace SqlDsl.SqlBuilders.SqlStatementParts
{
    class CompositeKey : ICompositeKey
    {
        readonly ReadOnlyCollection<ISelectColumn> _columns;
        bool _tableVerified;

        public IQueryTable Table => GetQueryTable();

        public int Count => _columns.Count;

        public CompositeKey(IEnumerable<ISelectColumn> columns)
        {
            _tableVerified = false;
            _columns = columns?.ToList().AsReadOnly();

            if (_columns.Count == 0)
                throw new InvalidOperationException("You must specify at least one key column");
        }

        IQueryTable GetQueryTable()
        {
            if (_tableVerified)
                return _columns[0].IsRowNumberForTable;;

            var tables = _columns
                .Select(x => x.IsRowNumberForTable)
                .Distinct()
                .ToList();

            if (tables.Count != 1)
                throw new InvalidOperationException($"Composite keys must come from the same table. Tables: {tables.Select(t => t.Alias).JoinString(", ")}.");

            _tableVerified = true;
            return GetQueryTable();
        }

        public IEnumerator<ISelectColumn> GetEnumerator() => _columns.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _columns.GetEnumerator();
    }
}