using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders.SqlStatementParts
{
    /// <summary>
    /// A column in the SELECT statement
    /// </summary>
    class SelectColumn : SelectColumnBase
    {
        /// <summary>
        /// The index of the row number column for the table which exposes this column
        /// </summary>
        public override int RowNumberColumnIndex { get; }

        public SelectColumn(string alias, string tableAlias, bool isRowNumber, IQueryTables tables)
            : base(alias, isRowNumber)
        {
            RowNumberColumnIndex = tables[tableAlias].RowNumberColumnIndex;
        }
    }
}