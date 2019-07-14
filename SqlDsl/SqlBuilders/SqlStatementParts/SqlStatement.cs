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
        public IQueryTables Tables { get; }

        /// <inheritdoc />
        public ISelectColumns SelectColumns { get; }

        /// <inheritdoc />
        IEnumerable<(string mappedPropertyName, ICompositeKey primaryKey)> ISqlSelectStatement.MappedPropertiesToPrimaryKeys => MappedPropertiesToPrimaryKeys;

        static readonly IEnumerable<(string, ICompositeKey)> MappedPropertiesToPrimaryKeys = Enumerable.Empty<(string, ICompositeKey)>();

        public SqlStatement(SqlStatementBuilder builder)
        {
            Tables = new QueryTables(builder, this);
            SelectColumns = new SelectColumns(builder, Tables);
        }
    }
}