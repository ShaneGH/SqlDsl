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
            return InnerSqlStatementPartValues.JoinTables.Where(FilterJoinTable);

            bool FilterJoinTable(SqlStatementPartJoin join) => 
                RequiredColumns.Any(c => c.tableAlias == join.Alias || c.aggregatesToTable == join.Alias);
        }

        IEnumerable<SqlStatementPartSelect> GetSelectColumns()
        {
            return InnerSqlStatementPartValues.SelectColumns.Where(FilterSelectColumn);

            bool FilterSelectColumn(SqlStatementPartSelect select) => true;
                //RequiredColumns.Any(c => c.tableAlias == join.Alias || c.aggregatesToTable == join.Alias);
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

        // IEnumerable<string> GetLineage(string table, IEnumerable<string> complete)
        // {
        //     if (table == PrimaryTableAlias)
        //         return table.ToEnumerable();

        //     if (complete.Contains(table))
        //         return complete;

        //     var join = Joins
        //         .Where(j => j.alias == table)
        //         .AsNullable()
        //         .FirstOrDefault();

        //     if (join == null)
        //         throw new InvalidOperationException($"Cannot find join {table}.");

        //     return join.Value.queryObjectReferences
        //         .SelectMany(x => GetLineage(x, complete.Append(table)))
        //         .Append(table);
        // }
    }
}