using SqlDsl.Mapper;
using SqlDsl.Query;
using SqlDsl.SqlBuilders.SqlStatementParts;
using SqlDsl.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    /// <summary>
    /// A class to build sql statements
    /// </summary>
    public class FilteredSqlStatementBuilder : ISqlBuilder, ISqlStatementPartValues
    {
        static readonly IEnumerable<string> EmptyStrings = new string[0];

        readonly SqlStatementBuilder InnerSqlBuilder;
        readonly ISqlStatementPartValues InnerSqlStatementPartValues;
        readonly IEnumerable<(string tableAlias, string columnAlias, string aggregatesToTable)> RequiredColumns;

        public FilteredSqlStatementBuilder(SqlStatementBuilder innerSqlBuilder, IEnumerable<(string tableAlias, string columnAlias, string aggregatesToTable)> requiredColumns)
        {
            InnerSqlBuilder = innerSqlBuilder ?? throw new ArgumentNullException(nameof(innerSqlBuilder));
            InnerSqlStatementPartValues = innerSqlBuilder ?? throw new ArgumentNullException(nameof(innerSqlBuilder));
            RequiredColumns = requiredColumns?.Enumerate() ?? throw new ArgumentNullException(nameof(requiredColumns));
        }

        public string UniqueAlias => InnerSqlStatementPartValues.UniqueAlias;

        public string PrimaryTableAlias => InnerSqlStatementPartValues.PrimaryTableAlias;

        public IEnumerable<SqlStatementPartJoin> JoinTables => GetJoinTables();

        public ISqlStatement InnerStatement => InnerSqlStatementPartValues.InnerStatement;

        public ISqlFragmentBuilder SqlBuilder => InnerSqlStatementPartValues.SqlBuilder;

        public IEnumerable<SqlStatementPartSelect> SelectColumns => GetSelectColumns();

        public IEnumerable<(string rowIdColumnName, string resultClassProperty)> RowIdsForMappedProperties => InnerSqlStatementPartValues.RowIdsForMappedProperties;

        IEnumerable<SqlStatementPartJoin> GetJoinTables()
        {
            var requiredTables = RequiredColumns
                .SelectMany(c => new[] { c.tableAlias, c.aggregatesToTable })
                .RemoveNulls()
                .ToHashSet();

            foreach (var table in requiredTables.ToArray())
                requiredTables.AddRange(GetLineage(table, EmptyStrings));

            return InnerSqlStatementPartValues.JoinTables.Where(FilterJoinTable);

            bool FilterJoinTable(SqlStatementPartJoin join) => requiredTables.Contains(join.Alias);
        }

        IEnumerable<string> GetLineage(string table, IEnumerable<string> complete)
        {
            if (table == PrimaryTableAlias)
                return table.ToEnumerable();

            if (complete.Contains(table))
                return complete;

            var join = InnerSqlStatementPartValues.JoinTables
                .Where(j => j.Alias == table)
                .AsNullable()
                .FirstOrDefault();

            // TODO: add where and order by column tables

            if (join == null)
                throw new InvalidOperationException($"Cannot find join {table}.");

            return join.Value.QueryObjectReferences
                .SelectMany(x => GetLineage(x, complete.Append(table)))
                .Append(table);
        }

        IEnumerable<SqlStatementPartSelect> GetSelectColumns()
        {
            // TODO: add where and order by columns

            return GetRowIdColumns().Concat(_GetSelectColumns());
        }

        IEnumerable<SqlStatementPartSelect> GetRowIdColumns()
        {
            var tables = RequiredColumns
                .Select(t => t.aggregatesToTable ?? t.tableAlias)
                .SelectMany(t => GetLineage(t, EmptyStrings).Prepend(t))
                .ToHashSet();

            return InnerSqlStatementPartValues.SelectColumns
                .Where(x => x.IsRowId)
                .Where(x => x.RepresentsColumns.Any(c => tables.Contains(c.table)));
        }

        IEnumerable<SqlStatementPartSelect> _GetSelectColumns()
        {
            return InnerSqlStatementPartValues.SelectColumns
                .Where(x => !x.IsRowId)
                .Where(x => x.RepresentsColumns.Any(c1 => RequiredColumns.Any(c2 => c2.tableAlias == c1.table && c2.columnAlias == c1.column)));
        }

        public (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) ToSqlString()
        {
            

            return InnerSqlBuilder.ToSqlString();
        }

        // /// <summary>
        // /// Remove any tables from the query which are not in the requiredTableAliases list
        // /// </summary>
        // public void FilterUnusedTables(IEnumerable<string> requiredTableAliases)
        // {
        //     var tables = new HashSet<string>(requiredTableAliases
        //         .SelectMany(t => GetLineage(t, EmptyStrings)));

        //     if (Where != null)
        //         tables.AddRange(Where.Value.queryObjectReferences);
        //     foreach (var j in Joins)
        //         tables.AddRange(j.queryObjectReferences);
        //     foreach (var o in Ordering)
        //         tables.AddRange(o.queryObjectReferences);

        //     for (var i = _Joins.Count - 1; i >= 0; i--)
        //     {
        //         if (!tables.Contains(_Joins[i].alias))
        //             _Joins.RemoveAt(i);
        //     }

        //     for (var i = _Select.Count - 1; i >= 0; i--)
        //     {
        //         if (_Select[i].representsColumns.Any(c => !tables.Contains(c.table)))
        //             _Select.RemoveAt(i);
        //     }
        // }
    }
}