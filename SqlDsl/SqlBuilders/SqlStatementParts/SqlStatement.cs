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
    /// Describes a sql statement
    /// </summary>
    public class SqlStatement : ISqlStatement
    {
        /// <inheritdoc />
        public string UniqueAlias { get; }

        /// <inheritdoc />
        public IQueryTables Tables { get; }

        /// <inheritdoc />
        public ISelectColumns SelectColumns { get; }

        /// <inheritdoc />
        IEnumerable<(string mappedPropertyName, ISelectColumn rowNumberColumn)> ISqlSelectStatement.MappedPropertiesToRowNumbers => MappedPropertiesToRowNumbers;

        static readonly IEnumerable<(string, ISelectColumn)> MappedPropertiesToRowNumbers = Enumerable.Empty<(string, ISelectColumn)>();

        public SqlStatement(ISqlStatementPartValues builder)
        {
            UniqueAlias = builder.UniqueAlias;
            Tables = new QueryTables(builder, this);
            SelectColumns = new SelectColumns(builder, Tables);
        }

        /// <inheritdoc />
        public IQueryTable TryGetTableForColum(string columnAlias)
        {
            var col = SelectColumns.TryGetColumn(columnAlias);
            if (col == null) return null;

            return col.Table;
        }

        /// <inheritdoc />
        public IQueryTable GetTableForColum(string columnAlias)
        {
            var col = SelectColumns[columnAlias];
            return col.Table;
        }
    }
}