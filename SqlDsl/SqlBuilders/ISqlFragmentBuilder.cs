using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    public interface ISqlFragmentBuilder
    {
        /// <summary>
        /// Build a sql statement which selects * from a table and adds a unique row id named {rowIdAlias}
        /// </summary>
        (string setupSql, string sql) GetSelectTableSqlWithRowId(string tableName, string rowIdAlias);

        /// <summary>
        /// Wrap a table name in parenthesis which protects against illegal characters: []
        /// </summary>
        string WrapTable(string table);

        /// <summary>
        /// Wrap a column name in parenthesis which protects against illegal characters: []
        /// </summary>
        string WrapColumn(string column);

        /// <summary>
        /// Wrap a table or column alias in parenthesis which protects against illegal characters: []
        /// </summary>
        string WrapAlias(string alias);

        /// <summary>
        /// Combine 2 statements with an AND condition
        /// </summary>
        (string setupSql, string sql) BuildAndCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with an OR condition
        /// </summary>
        (string setupSql, string sql) BuildOrCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with an = condition
        /// </summary>
        (string setupSql, string sql) BuildEqualityCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with an <> condition
        /// </summary>
        (string setupSql, string sql) BuildNonEqualityCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with < condition
        /// </summary>
        (string setupSql, string sql) BuildLessThanCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with a <= condition
        /// </summary>
        (string setupSql, string sql) BuildLessThanEqualToCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with a > condition
        /// </summary>
        (string setupSql, string sql) BuildGreaterThanCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with a >= condition
        /// </summary>
        (string setupSql, string sql) BuildGreaterThanEqualToCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with an IN condition
        /// </summary>
        (string setupSql, string sql) BuildInCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 params with comma
        /// </summary>
        (string setupSql, string sql) BuildCommaCondition(string lhs, string rhs);

        /// <summary>
        /// Alias an item
        /// </summary>
        string BuildAlias(string lhs, string alias);
        
        /// <summary>
        /// Combine 2 params with +
        /// </summary>
        string BuildAddCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 params with -
        /// </summary>
        string BuildSubtractCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 params with *
        /// </summary>
        string BuildMultiplyCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 params with /
        /// </summary>
        string BuildDivideCondition(string lhs, string rhs);
    }
}
