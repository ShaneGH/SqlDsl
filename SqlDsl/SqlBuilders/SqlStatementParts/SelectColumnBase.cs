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
    abstract class SelectColumnBase : ISelectColumn
    {
        /// <summary>
        /// The alias of the column
        /// </summary>
        public string Alias { get; }

        /// <summary>
        /// If true, this column is a row number
        /// </summary>
        public bool IsRowNumber { get; }
        
        /// <summary>
        /// The index of the row number column for the table which exposes this column
        /// </summary>
        public abstract int RowNumberColumnIndex { get; }

        public SelectColumnBase(string alias, bool isRowNumber)
        {
            Alias = alias;
            IsRowNumber = isRowNumber;
        }
    }
}