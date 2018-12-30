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
    public abstract class SqlStatementBuilderBase : ISqlString
    {
        static readonly (string, (string, string, string)[]) Select1 = ("1", new (string, string, string)[0]);
        static readonly ConstructorInfo[] EmptyConstructorInfo = new ConstructorInfo[0];
        static readonly string NullString = null;

        public readonly ISqlSyntax SqlSyntax;
        
        /// <summary>
        /// The alias of the table in the SELECT clause
        /// </summary>
        public readonly string PrimaryTableAlias;

        /// <summary>
        /// A list of columns in the SELECT statement
        /// </summary>
        readonly List<SelectColumn> _Select = new List<SelectColumn>();

        /// <summary>
        /// A list of columns in the SELECT statement
        /// </summary>
        public IEnumerable<SelectColumn> Select => _Select.Skip(0);

        public SqlStatementBuilderBase(ISqlSyntax sqlFragmentBuilder, string primaryTableAlias)
        {
            SqlSyntax = sqlFragmentBuilder ?? throw new ArgumentNullException(nameof(sqlFragmentBuilder));
            PrimaryTableAlias = primaryTableAlias ?? throw new ArgumentNullException(nameof(primaryTableAlias));
        }

        /// <summary>
        /// Add a column to the SELECT statement
        /// </summary>
        public void AddSelectColumn(Type cellDataType, string selectCode, string alias, (string table, string column, string aggregatedToTable)[] representsColumns, ConstructorInfo[] argConstructors = null) =>
            _Select.Add(new SelectColumn(cellDataType, selectCode, alias ?? throw new ArgumentNullException(nameof(alias)), representsColumns, argConstructors ?? EmptyConstructorInfo));

        /// <inheritdoc />
        public (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) ToSqlString() => ToSqlStringInternal(null, null);

        /// <inheritdoc />
        public (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) ToSqlString(IEnumerable<string> selectColumnAliases, IEnumerable<string> ensureTableRowIds)
        {
            return ToSqlStringInternal(
                selectColumnAliases ?? throw new ArgumentNullException(nameof(selectColumnAliases)), 
                ensureTableRowIds ?? throw new ArgumentNullException(nameof(ensureTableRowIds)));
        }

        (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) ToSqlStringInternal(IEnumerable<string> selectColumnAliases, IEnumerable<string> ensureTableRowIds)
        {
            var rowIds = GetRowIdSelectColumns(selectColumnAliases, ensureTableRowIds).Enumerate();
            if (!rowIds.Any())
            {
                // there must be at least 1 row id
                rowIds = GetRowIdSelectColumns()
                    .Take(1)
                    .Enumerate();
            }

            IEnumerable<SelectColumn> selects = _Select;
            if (selectColumnAliases != null)
            {
                var sca = selectColumnAliases.ToHashSet();
                selects = selects.Where(c => sca.Contains(c.Alias));
            }

            // TODO: look at $"{rid.tableAlias}.{rid.rowIdColumnName}". What if tableAlias is null or ##root
            var sels = rowIds
                .Select(rid => (sql: BuildSqlForRid(rid), representsColumns: new [] { (table: rid.tableAlias, NullString, aggregatedToTable: NullString) }))
                .Concat(selects.Select(sel => (sql: SqlSyntax.AddAliasColumn(sel.SelectCode, sel.Alias), representsColumns: sel.RepresentsColumns)))
                .ToList();

            // if there is absolutely nothing to select, prevent error by selecting 1
            if (sels.Count == 0)
                sels.Add(Select1);

            return _ToSqlString(
                sels.Select(c => c.sql), 
                sels
                    .SelectMany(s => s.representsColumns)
                    .SelectMany(c => new [] { c.table, c.aggregatedToTable })
                    .RemoveNulls()
                    .Distinct());

            string BuildSqlForRid((string rowIdColumnName, string tableAlias, string rowIdColumnNameAlias) rid)
            {
                var select = SqlSyntax.BuildSelectColumn(rid.tableAlias, rid.rowIdColumnName);
                var alias = rid.tableAlias == null
                    ? rid.rowIdColumnName
                    : rid.rowIdColumnName == null
                        ? rid.tableAlias
                        : $"{rid.tableAlias}.{rid.rowIdColumnName}";

                return SqlSyntax.AddAliasColumn(select, alias);
            }
        }

        protected abstract (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) _ToSqlString(IEnumerable<string> selectColumns, IEnumerable<string> selectTables);

        /// <summary>
        /// Concat DB table columns with row id columns
        /// </summary>
        protected IEnumerable<(bool isRowId, SelectColumn col)> GetAllSelectColumns() =>
            GetRowIdSelectColumns()
            .Select(x => (true, new SelectColumn((Type)null, SqlSyntax.BuildSelectColumn(x.tableAlias, x.rowIdColumnName), x.rowIdColumnNameAlias, new [] { (x.tableAlias, x.rowIdColumnName, NullString) }, EmptyConstructorInfo)))
            .Concat(_Select.Select(x => (false, x)));

        /// <summary>
        /// Get a list of row id colums, the alias of the table they are identifying, and the alias for the row id column (if any)
        /// </summary>
        protected abstract IEnumerable<(string rowIdColumnName, string tableAlias, string rowIdColumnNameAlias)> GetRowIdSelectColumns(IEnumerable<string> selectColumnAliases = null, IEnumerable<string> ensureTableRowIds = null);

        public class SelectColumn
        {
            public readonly Type CellDataType;
            public readonly string SelectCode;
            public readonly  string Alias;
            public readonly  (string table, string column, string aggregatedToTable)[] RepresentsColumns;
            public readonly  ConstructorInfo[] ArgConstructors;

            public SelectColumn(
                Type cellDataType, 
                string selectCode, 
                string alias, 
                (string table, string column, string aggregatedToTable)[] representsColumns, 
                ConstructorInfo[] argConstructors)
            {
                CellDataType = cellDataType;
                SelectCode = selectCode;
                Alias = alias;
                RepresentsColumns = representsColumns;
                ArgConstructors = argConstructors;
            }
        }
    }
}