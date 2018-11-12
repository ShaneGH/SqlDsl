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
        /// <inheritdoc />
        public string Alias { get; }

        /// <inheritdoc />
        public bool IsRowNumber { get; }

        /// <inheritdoc />
        public Type DataType { get; }
        
         /// <inheritdoc />
        public ConstructorInfo[] ArgConstructors { get; }
        
        /// <summary>
        /// The index of the row number column for the table which exposes this column
        /// </summary>
        public abstract int RowNumberColumnIndex { get; }

        public SelectColumnBase(string alias, bool isRowNumber, Type dataType, ConstructorInfo[] argConstructors)
        {
            Alias = alias;
            IsRowNumber = isRowNumber;
            DataType = dataType;
            ArgConstructors = argConstructors;
        }
    }
}