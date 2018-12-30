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
        // TODO: this class is unnecessary

        public SelectColumn((string table, string column, string aggregatedToTable)[] referencesColumns, string alias, IEnumerable<string> tableAliases, bool isRowNumber, Type dataType, ConstructorInfo[] argConstructors, IQueryTables tables)
            : base(referencesColumns, alias, isRowNumber, dataType, argConstructors, tables)
        {
        }
    }
}