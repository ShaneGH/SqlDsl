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

        /// <summary>
        /// Build a list of columns
        /// </summary>
        static IEnumerable<ISelectColumn> BuildColumns(SqlStatementBuilder queryBuilder, IQueryTables tables)
        {
            var hasInnerQuery = queryBuilder.InnerStatement != null;
            var cols = queryBuilder.Select.Select(BuildColumn);
            var ridCols = hasInnerQuery ?
                queryBuilder.InnerStatement.SelectColumns.Where(IsRowNumber) :
                tables.Select(BuildRowIdColumn);

            return ridCols.Concat(cols);

            bool IsRowNumber(ISelectColumn col) => col.IsRowNumber;

            ISelectColumn _BuildColumn((Type cellDataType, string columnName, string tableName, string alias, ConstructorInfo constructor) col, bool isRowId) =>
                (col.columnName ?? "").StartsWith("@") ?
                    new ConstSelectColumn(col.alias, isRowId, col.cellDataType, col.constructor) as ISelectColumn :
                    hasInnerQuery ?
                        new InnerQuerySelectColumn(col.columnName, col.alias ?? col.columnName, isRowId, col.cellDataType, col.constructor, queryBuilder) :
                        new SelectColumn(col.alias ?? col.columnName, col.tableName, isRowId, col.cellDataType, col.constructor, tables) as ISelectColumn;

            ISelectColumn BuildColumn((Type cellDataType, string columnName, string tableName, string alias, ConstructorInfo constructor) col) => _BuildColumn(col, false);

            ISelectColumn BuildRowIdColumn(IQueryTable table)
            {
                var columnAlias = table.Alias == SqlStatementConstants.RootObjectAlias ?
                    SqlStatementConstants.RowIdName :
                    $"{table.Alias}.{SqlStatementConstants.RowIdName}";

                return _BuildColumn((null, SqlStatementConstants.RowIdName, table.Alias, columnAlias, null), true);
            }
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
            foreach (var col in this)
            {
                if (col.Alias == alias)
                    return col;
            }

            throw new InvalidOperationException($"There is no column with alias: \"{alias}\".");
        }
    }
}