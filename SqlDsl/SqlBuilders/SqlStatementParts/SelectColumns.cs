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
    /// A list of columns in the SELECT statement
    /// </summary>
    class SelectColumns : ISelectColumns
    {
        readonly IEnumerable<ISelectColumn> Columns;

        public SelectColumns(ISqlStatementPartValues queryBuilder, IQueryTables tables)
        {
            // TODO: this constructor is called a lot. Why??

            Columns = BuildColumns(queryBuilder, tables).Enumerate();
        }

        /// <summary>
        /// Get a column based on it's index in the select statement
        /// </summary>
        public ISelectColumn this[int index] => GetColumn(index);

        /// <summary>
        /// Get a column based on it's alias
        /// </summary>
        public ISelectColumn this[string alias] => GetColumn(alias);

        static readonly string NullString = null;

        /// <summary>
        /// Build a list of columns
        /// </summary>
        static IEnumerable<ISelectColumn> BuildColumns(ISqlStatementPartValues queryParts, IQueryTables tables)
        {
            var hasInnerQuery = queryParts.InnerStatement != null;
            return queryParts.SelectColumns.Select(BuildColumn);

            ISelectColumn BuildColumn(SqlStatementPartSelect col) => 
                hasInnerQuery ?
                    new InnerQuerySelectColumn(col.RepresentsColumns, col.Alias, col.IsRowId, col.CellDataType, col.ArgConstructors, queryParts) :
                    (ISelectColumn)new SelectColumn(col.RepresentsColumns, col.Alias, col.RepresentsColumns.Select(x => x.table), col.IsRowId, col.CellDataType, col.ArgConstructors, tables);
        }

        public IEnumerator<ISelectColumn> GetEnumerator() => Columns.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => Columns.GetEnumerator();

        /// <summary>
        /// Get a column based on it's index in the select statement
        /// </summary>
        ISelectColumn GetColumn(int index)
        {
            var i = index;
            foreach (var col in this)
            {
                if (i == 0)
                    return col;

                i--;
            }

            throw new InvalidOperationException($"There is no column at index: {index}.");
        }

        /// <summary>
        /// Get a column based on it's alias
        /// </summary>
        ISelectColumn GetColumn(string alias)
        {
            var col = TryGetColumn(alias);
            if (col != null)
                return col;

            throw new InvalidOperationException($"There is no column with alias: \"{alias}\".");
        }

        public ISelectColumn TryGetColumn(string alias)
        {
            foreach (var col in this)
            {
                if (col.Alias == alias)
                    return col;
            }

            return null;
        }
    }
}