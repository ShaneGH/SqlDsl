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

        public SelectColumns(SqlStatementBuilder queryBuilder, IQueryTables tables)
        {
            // TODO: this constructor is called a lot. Why??

            Columns = BuildColumns(queryBuilder, tables).Enumerate();
        }

        /// <summary>
        /// Get a column based on it's alias
        /// </summary>
        public ISelectColumn this[string alias] => GetColumn(alias);
        
        /// <summary>
        /// Build a list of columns
        /// </summary>
        static IEnumerable<ISelectColumn> BuildColumns(SqlStatementBuilder queryParts, IQueryTables tables)
        {
            return queryParts.AllSelectColumns.Select(BuildColumn);

            ISelectColumn BuildColumn((bool isRowId, SqlStatementBuilder.SelectColumn col) col) => 
                new SelectColumn(col.col.RepresentsColumns, col.Item2.Alias, col.isRowId, col.col.CellDataType, col.col.ArgConstructors, tables);
        }

        public IEnumerator<ISelectColumn> GetEnumerator() => Columns.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => Columns.GetEnumerator();

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