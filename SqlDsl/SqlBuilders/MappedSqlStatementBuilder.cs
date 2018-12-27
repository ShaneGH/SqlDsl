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
    public class MappedSqlStatementBuilder : SqlStatementBuilderBase, SqlStatementParts.ISqlStatementPartValues
    {
        /// <summary>
        /// The inner query used in the SELECT clause
        /// </summary>
        readonly ISqlString InnerQueryBuilder;
        
        /// <summary>
        /// The inner query used in the SELECT clause
        /// </summary>
        readonly ISqlStatement InnerQueryStatement;

        /// <summary>
        /// A map from a row id column to a location in a mapped property graph
        /// </summary>
        public readonly List<(string rowIdColumnName, string resultClassProperty)> RowIdsForMappedProperties = new List<(string, string)>();

        protected override bool AliasRowIdColumns => false;

        bool SelectHasAggregation => Select
            .SelectMany(s => s.RepresentsColumns)
            .Any(x => x.aggregatedToTable != null);

        public MappedSqlStatementBuilder(ISqlSyntax sqlFragmentBuilder, ISqlString innerQueryBuilder, ISqlStatement innerQueryStatement, string primaryTableAlias)
            : base(sqlFragmentBuilder, primaryTableAlias)
        {
            InnerQueryBuilder = innerQueryBuilder ?? throw new ArgumentNullException(nameof(innerQueryBuilder));
            InnerQueryStatement = innerQueryStatement ?? throw new ArgumentNullException(nameof(innerQueryStatement));
        }

        /// <inheritdoc />
        protected override (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) _ToSqlString(IEnumerable<string> selectColumns, IEnumerable<string> selectTables)
        {
            // if a table is used as part of a mapping, but none of it's fields are used, we might need
            // to tell the inner query builder
            var usedTables = GetUsedTables(null, null).Select(t => t.Alias);
            var (querySetupSql, beforeWhereSql, whereSql, afterWhereSql) = InnerQueryBuilder.ToSqlString(GetSelectColumns(), usedTables);

            beforeWhereSql = $"SELECT {selectColumns.JoinString(",")}\nFROM ({beforeWhereSql}";
            afterWhereSql = $"{afterWhereSql}) {SqlSyntax.WrapAlias(PrimaryTableAlias)}{BuildGroupByStatement("\n")}";

            return (querySetupSql, beforeWhereSql, whereSql, afterWhereSql);
        }

        IEnumerable<string> GetSelectColumns()
        {
            // get the sql from the inner query
            var cols = Select
                .SelectMany(s => s.RepresentsColumns.Select(c => c.column))
                .Where(c => !c.StartsWith("@"))
                .ToList();

            var tables = cols.Select(c => InnerQueryStatement.Tables[InnerQueryStatement.SelectColumns[c].RowNumberColumnIndex]);
            tables = tables.SelectMany(GetTableChain);

            var rids = tables.Select(t => GetAlias(t) + SqlStatementConstants.RowIdName);
            return  cols.Concat(rids).Distinct();
                
            string GetAlias(IQueryTable qt) => qt.Alias == SqlStatementConstants.RootObjectAlias ? "" : $"{qt.Alias}.";
        }
            
        static IEnumerable<IQueryTable> GetTableChain(IQueryTable table)
        {
            if (table.JoinedFrom == null)
                return table.ToEnumerable();

            return GetTableChain(table.JoinedFrom).Append(table);
        }

        string BuildGroupByStatement(string prefix)
        {
            if (GetAllSelectColumns().All(cs => cs.col.RepresentsColumns.All(c => c.aggregatedToTable == null)))
                return "";

            var output = new List<string>(16);
            foreach (var col in GetAllSelectColumns()
                .SelectMany(cs => cs.col.RepresentsColumns)
                .Where(c => c.aggregatedToTable == null))
            {
                output.Add(SqlSyntax.BuildSelectColumn(col.table, col.column));
            }

            if (output.Count > 0)
                prefix += "GROUP BY ";

            return $"{prefix}{output.JoinString(",")}";
        }

        /// <inheritdoc />
        IEnumerable<IQueryTable> GetUsedTables(IEnumerable<string> selectColumnAliases, IEnumerable<string> ensureTableRowIds)
        {
            var cols = Select.SelectMany(s => s.RepresentsColumns);
            if (selectColumnAliases != null)
            {
                var sca = selectColumnAliases.ToHashSet();
                cols = cols.Where(c => sca.Contains(c.column));
            }

            var tables = new HashSet<IQueryTable>();

            // add tables which were explicetly requested
            foreach (var tab in ensureTableRowIds.OrEmpty())
                tables.Add(InnerQueryStatement.Tables[tab]);

            // add tables which columns are aggregated to
            foreach (var col in cols.Where(c => c.aggregatedToTable != null))
                tables.Add(InnerQueryStatement.Tables[col.aggregatedToTable]);
                
            // add tables for select columns
            foreach (var col in cols.Where(c => !c.column.StartsWith("@") && c.aggregatedToTable == null))
            {
                var column = InnerQueryStatement.SelectColumns[col.column];
                tables.Add(InnerQueryStatement.Tables[column.RowNumberColumnIndex]);
            }

            // add tables which other tables depend on
            foreach (var tab in tables.ToList())
                tables.AddRange(GetTableChain(tab));

            // add tables used in mapping, but which may not have any select columns
            if (!SelectHasAggregation)
            {
                foreach (var rid in RowIdsForMappedProperties)
                    tables.Add(InnerQueryStatement.GetTableForColum(rid.rowIdColumnName));
            }

            var first = true;
            foreach (var table in InnerQueryStatement.Tables)
            {
                if (first)
                {
                    // ensure that the row id for the primary table is always returned
                    first = false;
                    yield return table;
                }
                else if (tables.Contains(table))
                {
                    yield return table;
                }
            }
        }

        /// <inheritdoc />
        protected override IEnumerable<(string rowIdColumnName, string tableAlias, string rowIdColumnNameAlias)> GetRowIdSelectColumns(IEnumerable<string> selectColumnAliases = null, IEnumerable<string> ensureTableRowIds = null)
        {
            // making an assumption later on in this method that these
            // values are null. If they are not null, these values will need to
            // be addressed again
            if (selectColumnAliases != null || ensureTableRowIds != null)
                throw new NotImplementedException();

            selectColumnAliases = Enumerable.Empty<string>();
            ensureTableRowIds = Select
                .SelectMany(s => s.RepresentsColumns)
                .Select(s => s.aggregatedToTable ?? InnerQueryStatement.TryGetTableForColum(s.column)?.Alias)
                .RemoveNulls()
                .Distinct();

            foreach (var table in GetUsedTables(selectColumnAliases, ensureTableRowIds))
            {
                // Get row id from the SELECT
                var alias = table.Alias == null || table.Alias == SqlStatementConstants.RootObjectAlias ? 
                    SqlStatementConstants.RowIdName : 
                    $"{table.Alias}.{SqlStatementConstants.RowIdName}";
                
                // the only row id will be [inner query alias].[##rowid]
                yield return (
                    InnerQueryStatement.SelectColumns[table.RowNumberColumnIndex].Alias, 
                    InnerQueryStatement.UniqueAlias, 
                    alias);
            }
        }
        
        #region ISqlStatementPartValues

        string ISqlStatementPartValues.UniqueAlias => null;

        string ISqlStatementPartValues.PrimaryTableAlias => PrimaryTableAlias;

        IEnumerable<SqlStatementPartJoin> ISqlStatementPartValues.JoinTables => Enumerable.Empty<SqlStatementPartJoin>();

        ISqlStatement ISqlStatementPartValues.InnerStatement => InnerQueryStatement;

        ISqlSyntax ISqlStatementPartValues.SqlSyntax => SqlSyntax;

        IEnumerable<SqlStatementPartSelect> ISqlStatementPartValues.SelectColumns => GetAllSelectColumns().Select(BuildSelectCol);

        IEnumerable<(string rowIdColumnName, string resultClassProperty)> ISqlStatementPartValues.RowIdsForMappedProperties => RowIdsForMappedProperties;

        static readonly Func<(string alias, string sql, string setupSql, IEnumerable<string> queryObjectReferences), SqlStatementPartJoin> BuildJoinTable = join =>
            new SqlStatementPartJoin(join.alias, join.queryObjectReferences);

        static readonly Func<(bool, SelectColumn), SqlStatementPartSelect> BuildSelectCol = select =>
            // TODO: string manipulation
            new SqlStatementPartSelect(select.Item1, select.Item2.CellDataType, select.Item2.Alias, select.Item2.RepresentsColumns, select.Item2.ArgConstructors);

        #endregion
    }
}