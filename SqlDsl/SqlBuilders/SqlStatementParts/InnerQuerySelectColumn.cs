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
        // TODO: this class is unnecessary

        public InnerQuerySelectColumn((string table, string column, string aggregatedToTable)[] representsColumns, string alias, bool isRowNumber, Type dataType, ConstructorInfo[] argConstructors, ISqlStatementPartValues qb, IQueryTables tables)
            : base(representsColumns, alias, isRowNumber, dataType, argConstructors, tables)
        {
        }
    }
}