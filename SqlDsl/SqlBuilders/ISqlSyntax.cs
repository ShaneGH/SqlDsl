using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    public interface ISqlSyntax
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
        /// The text for SUM in the sql flavour
        /// </summary>
        string SumFunctionName { get; }

        /// <summary>
        /// The text for AVG in the sql flavour
        /// </summary>
        string AverageFunctionName { get; }

        /// <summary>
        /// The text for MAX in the sql flavour
        /// </summary>
        string MaxFunctionName { get; }

        /// <summary>
        /// The text for MIN in the sql flavour
        /// </summary>
        string MinFunctionName { get; }

        /// <summary>
        /// Add a dense rank column to a query
        /// </summary>
        string AddDenseRank(IEnumerable<string> selectColumns, string denseRankAlias, IEnumerable<(string, OrderDirection)> orderByClauses, string restOfQuery);

        /// <summary>
        /// Build a sql statement which selects * from a table and adds a unique row id named {rowIdAlias}.
        /// if teardownSqlCanBeInlined == true, the teardown sql will be executed in the same query as the select
        /// otherwise it will be executed immediately after the select
        /// </summary>
        SelectTableSqlWithRowId GetSelectTableSqlWithRowId(string tableName, string rowIdAlias, IEnumerable<string> otherColumnNames);

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

    /// <summary>
    /// A class to describe a SELECT table
    /// </summary>
    public class SelectTableSqlWithRowId
    {
        /// <summary>
        /// If the table needs to execute a separate query outside the body of the main query, this value should be set.
        /// For example, a temp table may need to be created
        /// </summary>
        public readonly string SetupSql;

        /// <summary>
        /// The sql which points to the table
        /// </summary>
        public readonly string Sql;
        
        /// <summary>
        /// If any resources were created in SetupSql, these can be desposed of with a query here
        /// </summary>
        public readonly string TeardownSql;
        
        /// <summary>
        /// If the TeardownSql can be executed as part of the main query, this value
        /// should be true. If a separate qurey should be executed, this should be false
        /// </summary>
        public readonly bool TeardownSqlCanBeInlined;

        public SelectTableSqlWithRowId(string setupSql, string sql, string teardownSql, bool teardownSqlCanBeInlined)
        {
            SetupSql = setupSql;
            Sql = sql ?? throw new ArgumentNullException(nameof(sql));
            TeardownSql = teardownSql;
            TeardownSqlCanBeInlined = teardownSqlCanBeInlined;
        }

        public SelectTableSqlWithRowId(string setupSql, string sql)
            : this(setupSql, sql, null, true)
        {
        }

        public SelectTableSqlWithRowId(string sql)
            : this(null, sql)
        {
        }
    } 
}
