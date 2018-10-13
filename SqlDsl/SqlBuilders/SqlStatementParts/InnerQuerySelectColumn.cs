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
    /// Select column for a column in a wrapped query
    /// </summary>
    class InnerQuerySelectColumn : SelectColumnBase
    {
        /// <summary>
        /// The index of the row number column for the table which exposes this column
        /// </summary>
        public override int RowNumberColumnIndex { get; }

        public InnerQuerySelectColumn(string name, string alias, bool isRowNumber, SqlStatementBuilder qb)
            : base(alias, isRowNumber)
        {
            RowNumberColumnIndex = qb.InnerStatement.SelectColumns[name].RowNumberColumnIndex;
        }
    }
}