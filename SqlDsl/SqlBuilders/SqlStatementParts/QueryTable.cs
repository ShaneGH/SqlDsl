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
    /// A table within a query
    /// </summary>
    class QueryTable : IQueryTable
    {
        readonly SqlStatementBuilder QueryBuilder;

        readonly IQueryTables Tables;

        /// <summary>
        /// The table alias
        /// </summary>
        public string Alias { get; }

        /// <summary>
        /// The index of the column which provides row numbers for this table
        /// </summary>
        public int RowNumberColumnIndex { get; }

        /// <summary>
        /// If this table is in a join, will be the table that it is joined on.
        /// Otherwise it will be null
        /// </summary>
        public IQueryTable JoinedFrom => GetJoinedFrom();

        public QueryTable(string alias, SqlStatementBuilder queryBuilder, IQueryTables tables)
        {
            Alias = alias ?? throw new ArgumentNullException(nameof(alias));
            QueryBuilder = queryBuilder ?? throw new ArgumentNullException(nameof(queryBuilder));
            Tables = tables ?? throw new ArgumentNullException(nameof(tables));

            RowNumberColumnIndex = GetRowNumberColumnIndex(queryBuilder, alias);
        }

        /// <summary>
        /// Calling this method before full construction of the SqlStatement graph will
        /// cause instability
        /// </summary>
        IQueryTable GetJoinedFrom()
        {
            if (QueryBuilder.PrimaryTableAlias == Alias)
                return null;

            var table = QueryBuilder.Joins
                .Where(j => j.alias == Alias)
                // TODO: Will fail when a table is joined to multiple other tables
                .Select(x => x.queryObjectReferences.Single()).FirstOrDefault();

            if (table == null)
                throw new InvalidOperationException($"Cannot find join table with alias: {Alias}");

            return Tables[table];
        }

        /// <summary>
        /// The index of the column which provides row numbers for this table
        /// </summary>
        static int GetRowNumberColumnIndex(SqlStatementBuilder queryBuilder, string alias)
        {
            if (queryBuilder.PrimaryTableAlias == alias)
                return 0;

            var index = queryBuilder.Joins
                .Select(GetAlias)
                .Select(CombineWithIndex)
                .Where(FilterByAlias)
                .Select(GetIndex)
                .FirstOrDefault();

            // 0 will always be default value, because of "i + 1" clause in CombineWithIndex
            if (index == 0)
                throw new InvalidOperationException($"Could not find join table for alias: {alias}");

            return index;

            string GetAlias((string alias, string sql, string setupSql, IEnumerable<string> queryObjectReferences) val) => val.alias;

            (int, T) CombineWithIndex<T>(T val, int i) => (i + 1, val);

            bool FilterByAlias((int, string) val) => val.Item2 == alias;

            int GetIndex<T>((int, T) val) => val.Item1;
        }
    }
}