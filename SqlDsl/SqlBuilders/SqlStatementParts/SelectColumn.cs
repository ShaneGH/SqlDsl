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

        public SelectColumn((string table, string column, string aggregatedToTable)[] referencesColumns, string alias, IEnumerable<string> tableAliases, bool isRowNumber, Type dataType, ConstructorInfo[] argConstructors, IQueryTables tables)
            : base(referencesColumns, alias, isRowNumber, dataType, argConstructors)
        {
            int col = 0;
            foreach (var ta in tableAliases)
            {
                if (ta != null && !ta.StartsWith("@"))
                {
                    // TODO: Math.Max should work for now, but will not
                    // work when tables are joined multiple times
                    col = Math.Max(col, tables[ta].RowNumberColumnIndex);
                }
            }

            RowNumberColumnIndex = col;
        }
    }
}