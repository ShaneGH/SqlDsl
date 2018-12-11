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
        /// The text for DESCENDING in the sql flavour
        /// </summary>
        string Descending { get; }

        /// <summary>
        /// The text for COUNT in the sql flavour
        /// </summary>
        string CountFunctionName { get; }

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
        string BuildAndCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with an OR condition
        /// </summary>
        string BuildOrCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with an = condition
        /// </summary>
        string BuildEqualityCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with an <> condition
        /// </summary>
        string BuildNonEqualityCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with < condition
        /// </summary>
        string BuildLessThanCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with a <= condition
        /// </summary>
        string BuildLessThanEqualToCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with a > condition
        /// </summary>
        string BuildGreaterThanCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with a >= condition
        /// </summary>
        string BuildGreaterThanEqualToCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 statements with an IN condition
        /// </summary>
        string BuildInCondition(string lhs, string rhs);

        /// <summary>
        /// Combine 2 params with comma
        /// </summary>
        string BuildCommaCondition(string lhs, string rhs);

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
