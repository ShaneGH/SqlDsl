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
        /// <summary>
        /// Build a sql statement which selects * from a table and adds a unique row id named {rowIdAlias}
        /// </summary>
        public abstract (string setupSql, string sql) GetUniqueIdSql(string tableName, string rowIdAlias);

        /// <summary>
        /// Wrap a table name in parenthesis which protects against illegal characters: []
        /// </summary>
        public abstract string WrapTable(string table);

        /// <summary>
        /// Wrap a column name in parenthesis which protects against illegal characters: []
        /// </summary>
        public abstract string WrapColumn(string column);

        /// <summary>
        /// Wrap a table or column alias in parenthesis which protects against illegal characters: []
        /// </summary>
        public abstract string WrapAlias(string alias);

        /// <summary>
        /// Combine 2 statements with an AND condition
        /// </summary>
        public virtual (string setupSql, string sql) BuildAndCondition(string lhs, string rhs) => (null, $"({lhs} AND {rhs})");

        /// <summary>
        /// Combine 2 statements with an OR condition
        /// </summary>
        public virtual (string setupSql, string sql) BuildOrCondition(string lhs, string rhs) => (null, $"({lhs} OR {rhs})");

        /// <summary>
        /// Combine 2 statements with an = condition
        /// </summary>
        public virtual (string setupSql, string sql) BuildEqualityCondition(string lhs, string rhs) => (null, $"({lhs} = {rhs})");

        /// <summary>
        /// Combine 2 statements with an <> condition
        /// </summary>
        public virtual (string setupSql, string sql) BuildNonEqualityCondition(string lhs, string rhs) => (null, $"({lhs} <> {rhs})");

        /// <summary>
        /// Combine 2 statements with < condition
        /// </summary>
        public virtual (string setupSql, string sql) BuildLessThanCondition(string lhs, string rhs) => (null, $"({lhs} < {rhs})");

        /// <summary>
        /// Combine 2 statements with a <= condition
        /// </summary>
        public virtual (string setupSql, string sql) BuildLessThanEqualToCondition(string lhs, string rhs) => (null, $"({lhs} <= {rhs})");

        /// <summary>
        /// Combine 2 statements with a > condition
        /// </summary>
        public virtual (string setupSql, string sql) BuildGreaterThanCondition(string lhs, string rhs) => (null, $"({lhs} > {rhs})");

        /// <summary>
        /// Combine 2 statements with a >= condition
        /// </summary>
        public virtual (string setupSql, string sql) BuildGreaterThanEqualToCondition(string lhs, string rhs) => (null, $"({lhs} >= {rhs})");
    }
}
