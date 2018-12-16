using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    /// <summary>
    /// Base class for building sql statements. Inherit from this class to build different flavours of sql
    /// </summary>
    public abstract class SqlFragmentBuilderBase : ISqlFragmentBuilder
    {
        /// <inheritdoc />
        public virtual string BuildAlias(string lhs, string alias) => $"{lhs} AS {alias}";

        /// <inheritdoc />
        public abstract (string setupSql, string sql) GetSelectTableSqlWithRowId(string tableName, string rowIdAlias);

        /// <inheritdoc />
        public abstract string WrapTable(string table);

        /// <inheritdoc />
        public abstract string WrapColumn(string column);

        /// <inheritdoc />
        public abstract string WrapAlias(string alias);

        /// <inheritdoc />
        public virtual string BuildAndCondition(string lhs, string rhs) => $"({lhs} AND {rhs})";

        /// <inheritdoc />
        public virtual string BuildOrCondition(string lhs, string rhs) => $"({lhs} OR {rhs})";

        /// <inheritdoc />
        public virtual string BuildEqualityCondition(string lhs, string rhs) => $"({lhs} = {rhs})";

        /// <inheritdoc />
        public virtual string BuildNonEqualityCondition(string lhs, string rhs) => $"({lhs} <> {rhs})";

        /// <inheritdoc />
        public virtual string BuildLessThanCondition(string lhs, string rhs) => $"({lhs} < {rhs})";

        /// <inheritdoc />
        public virtual string BuildLessThanEqualToCondition(string lhs, string rhs) => $"({lhs} <= {rhs})";

        /// <inheritdoc />
        public virtual string BuildGreaterThanCondition(string lhs, string rhs) => $"({lhs} > {rhs})";

        /// <inheritdoc />
        public virtual string BuildGreaterThanEqualToCondition(string lhs, string rhs) => $"({lhs} >= {rhs})";

        /// <inheritdoc />
        public virtual string BuildInCondition(string lhs, string rhs) => $"({lhs} IN ({rhs}))";

        /// <inheritdoc />
        public virtual string BuildCommaCondition(string lhs, string rhs) => $"{lhs}, {rhs}";

        /// <inheritdoc />
        public virtual string BuildAddCondition(string lhs, string rhs) => $"({lhs} + {rhs})";

        /// <inheritdoc />
        public virtual string BuildSubtractCondition(string lhs, string rhs) => $"({lhs} - {rhs})";

        /// <inheritdoc />
        public virtual string BuildMultiplyCondition(string lhs, string rhs) => $"({lhs} * {rhs})";

        /// <inheritdoc />
        public virtual string BuildDivideCondition(string lhs, string rhs) => $"({lhs} / {rhs})";

        // TODO: other operators (e.g. OR)
    }
}
