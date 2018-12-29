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
    /// A list of tables in the query.false (FROM and JOIN)
    /// </summary>
    class QueryTables : IQueryTables
    {
        readonly IEnumerable<IQueryTable> Tables;

        public QueryTables(ISqlStatementPartValues queryBuilder, ISqlStatement sqlStatement)
        {
            Tables = BuildTables(queryBuilder, this, sqlStatement).ToArray();
        }

        /// <summary>
        /// Get a table based on the index of its row number column
        /// </summary>
        public IQueryTable this[int rowNumberColumnIndex] => TryGetTable(rowNumberColumnIndex) ??
            throw new InvalidOperationException($"There is no table with row number column index: {rowNumberColumnIndex}.");

        /// <summary>
        /// Get a table based on it's index
        /// </summary>
        public IQueryTable this[string alias] => TryGetTable(alias) ??
            throw new InvalidOperationException($"There is no table with alias: {alias}.");

        public IEnumerator<IQueryTable> GetEnumerator() => Tables.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Return a table object for the SELECT and each JOIN
        /// </summary>
        static IEnumerable<IQueryTable> BuildTables(ISqlStatementPartValues queryBuilder, IQueryTables tables, ISqlStatement sqlStatement)
        {
            yield return new QueryTable(queryBuilder.PrimaryTableAlias, queryBuilder, tables, sqlStatement);

            foreach (var j in queryBuilder.JoinTables)
                yield return new QueryTable(j.Alias, queryBuilder, tables, sqlStatement);
        }

        /// <inheritdoc />
        public IQueryTable TryGetTable(int rowNumberColumnIndex)
        {
            foreach (var tab in this)
            {
                if (tab.RowNumberColumnIndex == rowNumberColumnIndex)
                    return tab;
            }

            return null;
        }

        /// <inheritdoc />
        public IQueryTable TryGetTable(string alias)
        {
            foreach (var tab in this)
            {
                if (tab.Alias == alias)
                    return tab;
            }

            return null;
        }
    }
}