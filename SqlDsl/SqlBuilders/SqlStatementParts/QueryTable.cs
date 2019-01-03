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
        /// If this table is in a join, will be the table that it is joined on.
        /// Otherwise it will be null
        /// </summary>
        public IQueryTable JoinedFrom => GetJoinedFrom();

        ISelectColumn _RowNumberColumn;

        /// <inheritdoc />
        public ISelectColumn RowNumberColumn => _RowNumberColumn ??
            (_RowNumberColumn = GetRowNumberColumn());

        readonly ISqlStatement ParentStatement;

        public QueryTable(string alias, SqlStatementBuilder queryBuilder, IQueryTables tables, ISqlStatement parentStatement)
        {
            Alias = alias ?? throw new ArgumentNullException(nameof(alias));
            QueryBuilder = queryBuilder ?? throw new ArgumentNullException(nameof(queryBuilder));
            Tables = tables ?? throw new ArgumentNullException(nameof(tables));

            ParentStatement = parentStatement ?? throw new ArgumentNullException(nameof(parentStatement));
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
        /// The column which provides row numbers for this table
        /// </summary>
        ISelectColumn GetRowNumberColumn()
        {
            if (ParentStatement.SelectColumns == null)
                throw new InvalidOperationException("Column accessed before ParentStatment initialized.");

            var alias = Alias == SqlStatementConstants.RootObjectAlias
                ? SqlStatementConstants.RowIdName
                : $"{Alias}.{SqlStatementConstants.RowIdName}";

            return ParentStatement.SelectColumns[alias];
        }
    }
}