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
    /// A column in the SELECT statement which does not refer to a table column
    /// </summary>
    class ConstSelectColumn : SelectColumnBase
    {
        /// <summary>
        /// The index of the row number column for the table which exposes this column
        /// </summary>
        public override int RowNumberColumnIndex => 0;

        public ConstSelectColumn(string alias, bool isRowNumber, Type dataType, ConstructorInfo[] argConstructors)
            : base(alias, isRowNumber, dataType, argConstructors)
        {
        }
    }
}