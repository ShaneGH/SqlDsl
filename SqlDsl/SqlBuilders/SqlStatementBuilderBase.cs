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
        static readonly ConstructorInfo[] EmptyConstructorInfo = new ConstructorInfo[0];

        static readonly string NullString = null;

        public readonly ISqlSyntax SqlBuilder;
        
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
            SqlBuilder = sqlFragmentBuilder ?? throw new ArgumentNullException(nameof(sqlFragmentBuilder));
            PrimaryTableAlias = primaryTableAlias ?? throw new ArgumentNullException(nameof(primaryTableAlias));
        }

        /// <summary>
        /// Add a column to the SELECT statement
        /// </summary>
        public void AddSelectColumn(Type cellDataType, string selectCode, string alias, (string table, string column, string aggregatedToTable)[] representsColumns, ConstructorInfo[] argConstructors = null) =>
            _Select.Add(new SelectColumn(cellDataType, selectCode, alias ?? throw new ArgumentNullException(nameof(alias)), representsColumns, argConstructors ?? EmptyConstructorInfo));


        /// <inheritdoc />
        public (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) ToSqlString()
        {
            // build SELECT columns (cols and row ids)
            var select = GetAllSelectColumns()
                .Select(s => SqlBuilder.AddAliasColumn(s.col.SelectCode, s.col.Alias))
                .Enumerate();

            // add placeholder in case no SELECT columns were specified
            if (!select.Any())
                select = new [] { "1" };

            return ToSqlString(select);
        }

        protected abstract (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) ToSqlString(IEnumerable<string> selectColumns);

        /// <summary>
        /// Concat DB table columns with row id columns
        /// </summary>
        protected IEnumerable<(bool isRowId, SelectColumn col)> GetAllSelectColumns() =>
            GetRowIdSelectColumns()
            .Select(x => (true, new SelectColumn((Type)null, SqlBuilder.BuildSelectColumn(x.tableAlias, x.rowIdColumnName), x.rowIdColumnNameAlias, new [] { (x.tableAlias, x.rowIdColumnName, NullString) }, EmptyConstructorInfo)))
            .Concat(_Select.Select(x => (false, x)));

        /// <summary>
        /// Get a list of row id colums, the alias of the table they are identifying, and the alias for the row id column (if any)
        /// </summary>
        protected abstract IEnumerable<(string rowIdColumnName, string tableAlias, string rowIdColumnNameAlias)> GetRowIdSelectColumns();

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