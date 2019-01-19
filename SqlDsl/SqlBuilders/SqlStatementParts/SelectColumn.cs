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
    class SelectColumn : ISelectColumn
    {
        /// <inheritdoc />
        public string Alias { get; }
        readonly (string table, string column, string aggregatedToTable)[] ReferencesColumns;
        readonly IQueryTables Tables;
        IQueryTable _Table;

        /// <inheritdoc />
        public IQueryTable IsRowNumberForTable => Table.RowNumberColumn == this ? Table : null;

        /// <inheritdoc />
        public Type DataType { get; }
        
         /// <inheritdoc />
        public ConstructorInfo[] ArgConstructors { get; }

        /// <inheritdoc />
        public ISelectColumn RowNumberColumn => Table.RowNumberColumn;

        /// <inheritdoc />
        public bool IsRowNumber => IsRowNumberForTable != null;

        IQueryTable Table => _Table ?? (_Table = GetRowIdSelectTable());

        /// <summary>
        /// The mapping context for an unmapped column is it's own table
        /// </summary>
        public IQueryTable MappingContext => Table;

        public SelectColumn(
            (string table, string column, string aggregatedToTable)[] referencesColumns, 
            string alias, 
            bool isRowNumber, 
            Type dataType, 
            ConstructorInfo[] argConstructors, 
            IQueryTables tables)
        {
            Alias = alias;
            DataType = dataType;
            ArgConstructors = argConstructors;
            Tables = tables ?? throw new ArgumentNullException(nameof(tables));
            ReferencesColumns = referencesColumns ?? throw new ArgumentNullException(nameof(referencesColumns));
        }

        IQueryTable GetRowIdSelectTable()
        {
            return ReferencesColumns
                .Where(t => t.table != null)
                .Select(t => Tables[t.table])
                .FirstOrDefault() ??
                throw new InvalidOperationException($"Cannot find table for column {Alias}");
        }
    }
}