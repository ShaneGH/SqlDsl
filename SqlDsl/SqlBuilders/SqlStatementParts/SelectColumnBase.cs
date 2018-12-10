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
        
         /// <inheritdoc />
        public abstract int RowNumberColumnIndex { get; }

         /// <inheritdoc />
        public (string table, string column)[] ReferencesColumns { get; }

        public SelectColumnBase((string table, string column)[] referencesColumns, string alias, bool isRowNumber, Type dataType, ConstructorInfo[] argConstructors)
        {
            ReferencesColumns = referencesColumns;
            Alias = alias;
            IsRowNumber = isRowNumber;
            DataType = dataType;
            ArgConstructors = argConstructors;
        }
    }
}