using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.SqlBuilders.SqlStatementParts
{
    /// <summary>
    /// A table within a query
    /// </summary>
    class QueryTable : IQueryTable
    {
        readonly object _lock = new object();

        readonly SqlStatementBuilder QueryBuilder;

        readonly IQueryTables Tables;

        /// <summary>
        /// The table alias
        /// </summary>
        public string Alias { get; }

        IQueryTable[] _JoinedFrom = null;

        /// <summary>
        /// If this table is in a join, will be the table that it is joined on.
        /// Otherwise it will be null
        /// </summary>
        public IQueryTable[] JoinedFrom => GetJoindFrom();

        ICompositeKey _PrimaryKey;

        /// <inheritdoc />
        public ICompositeKey PrimaryKey => GetPrimaryKey();

        /// <inheritdoc />
        public JoinType? JoinType { get; }

        readonly ISqlStatement ParentStatement;

        readonly int ExplicitPrimaryKeys;

        public QueryTable(string alias, int explicitPrimaryKeys, JoinType? joinType, SqlStatementBuilder queryBuilder, IQueryTables tables, ISqlStatement parentStatement)
        {
            Alias = alias ?? throw new ArgumentNullException(nameof(alias));
            ExplicitPrimaryKeys = explicitPrimaryKeys;
            JoinType = joinType;
            QueryBuilder = queryBuilder ?? throw new ArgumentNullException(nameof(queryBuilder));
            Tables = tables ?? throw new ArgumentNullException(nameof(tables));

            ParentStatement = parentStatement ?? throw new ArgumentNullException(nameof(parentStatement));
        }

        /// <summary>
        /// Calling this method before full construction of the SqlStatement graph will
        /// cause instability
        /// </summary>
        IEnumerable<IQueryTable> BuildJoinedFrom()
        {
            if (QueryBuilder.PrimaryTableAlias == Alias)
                return CodingConstants.Empty.IQueryTable;

            return QueryBuilder.Joins
                .Where(j => j.Alias == Alias)
                .Select(x => x.QueryObjectReferences)
                .FirstOrDefault()
                ?.Select(t => Tables[t])
                .ToArray() ?? throw new InvalidOperationException($"Cannot find join table with alias: {Alias}");
        }

        IQueryTable[] GetJoindFrom()
        {
            lock (_lock)
            {
                return _JoinedFrom ?? (_JoinedFrom = BuildJoinedFrom().ToArray());                
            }
        }

        /// <summary>
        /// The column which provides row numbers for this table
        /// </summary>
        ICompositeKey BuildPrimaryKey()
        {
            if (ParentStatement.SelectColumns == null)
                throw new InvalidOperationException("Column accessed before ParentStatment initialized.");

            var pks = IEnumerableUtils.Create(
                Math.Max(1, ExplicitPrimaryKeys),
                GetPk);

            return new CompositeKey(pks);

            ISelectColumn GetPk(int index)
            {
                var alias = Alias == SqlStatementConstants.RootObjectAlias
                    ? SqlStatementConstants.PrimaryKeyName + index
                    : $"{Alias}.{SqlStatementConstants.PrimaryKeyName}{index}";

                return ParentStatement.SelectColumns[alias];
            }
        }

        ICompositeKey GetPrimaryKey()
        {
            lock (_lock)
            {
                return _PrimaryKey ?? (_PrimaryKey = BuildPrimaryKey());
                
            }
        }
    }
}