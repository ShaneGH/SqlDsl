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
    class MappedSqlStatementBuilder : ISqlString
    {
        readonly BuildMapState State;
        readonly Dictionary<string, QueryElementBasedMappedProperty> SelectProperties;
        readonly ISqlSelectStatement Statement;
        readonly ISqlString InnerSqlString;
        readonly string InnerQueryAlias;
        readonly ISqlSyntax SqlSyntax;

        IEnumerable<TheAmazingElement> AllItemsInAllProperties => SelectProperties.SelectMany(cs => cs.Value.FromParams.GetEnumerable1());
        IEnumerable<TheAmazingElement> AllNonParametersInAllProperties => AllItemsInAllProperties.Where(p => !p.IsParameter);

        public MappedSqlStatementBuilder(
            BuildMapState state, 
            IEnumerable<QueryElementBasedMappedProperty> selectProperties, 
            ISqlSelectStatement statement, 
            ISqlString innerSqlString,
            ISqlSyntax sqlSyntax)
        {
            State = state ?? throw new ArgumentNullException(nameof(state));
            SelectProperties = selectProperties?.ToDictionary(x => x.To);
            Statement = statement ?? throw new ArgumentNullException(nameof(statement));
            InnerSqlString = innerSqlString ?? throw new ArgumentNullException(nameof(innerSqlString));
            SqlSyntax = sqlSyntax ?? throw new ArgumentNullException(nameof(innerSqlString));

            // this paradigm can be extended if there are more than 1
            // alias needed per query
            InnerQueryAlias = SqlStatementConstants.InnerQueryAlias;
        }

        /// <inheritdoc />
        public (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) ToSqlString(IEnumerable<string> selectColumnAliases = null)
        {
            // if a table is used as part of a mapping, but none of it's fields are used, we might need
            // to tell the inner query builder
            var usedColumns = GetUsedColumns().Select(t => t.Alias);
            var usedTables = GetUsedTables().Select(t => t.Alias);
            var (querySetupSql, beforeWhereSql, whereSql, afterWhereSql) = InnerSqlString.ToSqlString(usedColumns);

            beforeWhereSql = $"SELECT {GetSelectColumns(selectColumnAliases).JoinString(",")}\nFROM ({beforeWhereSql}";
            afterWhereSql = $"{afterWhereSql}) {SqlSyntax.WrapAlias(InnerQueryAlias)}{BuildGroupByStatement("\n")}";

            return (querySetupSql, beforeWhereSql, whereSql, afterWhereSql);
        }

        IEnumerable<string> GetSelectColumns(IEnumerable<string> selectColumnAliases)
        {
            var selCols = selectColumnAliases?.ToHashSet();
            var cols = selectColumnAliases != null
                ? Statement.SelectColumns.Where(x => selCols.Contains(x.Alias))
                : Statement.SelectColumns as IEnumerable<ISelectColumn>;

            return cols.Select(GetSelectColumn);
        }

        string GetSelectColumn(ISelectColumn c)
        {
            if (c.IsRowNumber)
                return SqlSyntax.BuildSelectColumn(InnerQueryAlias, c.Alias);

            if (!SelectProperties.TryGetValue(c.Alias, out QueryElementBasedMappedProperty el))
                throw new InvalidOperationException($"Cannot find column with alias: {c.Alias}.");

            return SqlSyntax.AddAliasColumn(
                el.FromParams.BuildFromString(State, SqlSyntax, InnerQueryAlias),
                el.To);
        }

        IEnumerable<IQueryTable> GetUsedTables()
        {
            return AllNonParametersInAllProperties
                .SelectMany(c => new [] { c.Column.Table, c.RowIdColumn.Table })
                .RemoveNulls()
                .Distinct();
        }

        IEnumerable<ISelectColumn> GetUsedColumns()
        {
            return AllNonParametersInAllProperties
                .SelectMany(c => new [] { c.Column, c.RowIdColumn })
                .RemoveNulls()
                .Concat(Statement.SelectColumns.Where(c => c.IsRowNumber))
                .Distinct();
        }

        string BuildGroupByStatement(string prefix)
        {
            if (!AllNonParametersInAllProperties.Any(x => x.ColumnIsAggregatedToDifferentTable))
                return "";

            var groupByCols = AllNonParametersInAllProperties
                .Where(c => !c.ColumnIsAggregatedToDifferentTable)
                .Select(c => c.Column)
                // TODO: what if the column is grouped by also
                .Concat(Statement.SelectColumns.Where(c => c.IsRowNumber));

            var output = new List<string>(16);
            foreach (var col in groupByCols)
            {
                output.Add(SqlSyntax.BuildSelectColumn(InnerQueryAlias, col.Alias));
            }

            if (output.Count > 0)
                prefix += "GROUP BY ";

            return $"{prefix}{output.JoinString(",")}";
        }
    }
}