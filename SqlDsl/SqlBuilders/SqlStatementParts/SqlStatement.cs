using System.Collections.Generic;
using System.Linq;

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
        public ICompositeKey PrimaryKey { get; }

        /// <inheritdoc />
        IEnumerable<(string mappedPropertyName, ICompositeKey primaryKey)> ISqlSelectStatement.MappedPropertiesToPrimaryKeys => MappedPropertiesToPrimaryKeys;

        static readonly IEnumerable<(string, ICompositeKey)> MappedPropertiesToPrimaryKeys = Enumerable.Empty<(string, ICompositeKey)>();

        public SqlStatement(SqlStatementBuilder builder)
        {
            Tables = new QueryTables(builder, this);
            SelectColumns = new SelectColumns(builder, Tables);
            PrimaryKey = Tables[builder.PrimaryTableAlias].PrimaryKey;
        }
    }
}