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

        public InnerQuerySelectColumn((string table, string column, string aggregatedToTable)[] representsColumns, string alias, bool isRowNumber, Type dataType, ConstructorInfo[] argConstructors, SqlStatementBuilder qb)
            : base(representsColumns, alias, isRowNumber, dataType, argConstructors)
        {
            int col = 0;
            foreach (var ta in representsColumns)
            {
                if (ta.column != null && !ta.column.StartsWith("@"))
                {
                    // TODO: Math.Max should work for now, but will not
                    // work when tables are joined multiple times
                    col = Math.Max(col, qb.InnerStatement.SelectColumns[ta.column].RowNumberColumnIndex);
                }
            }

            RowNumberColumnIndex = col;
        }
    }
}