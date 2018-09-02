using SqlDsl.Query;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    /// <summary>
    /// Represents an object which can be converted into a sql statement
    /// </summary>
    public interface ISqlStatement
    {        
        /// <summary>
        /// A unique alias for this statement. Uniqueness must be guaranteed within a single statement only
        /// </summary>
        string UniqueAlias { get; }

        /// <summary>
        /// A map from column names to the name of the column of it's RowId
        /// </summary>
        IEnumerable<(string columnName, string rowIdColumnName)> RowIdMap { get; }

        /// <summary>
        /// A list of row id colums, the alias of the table they are identifying, and the alias for the row id column (if any)
        /// </summary>
        IEnumerable<(string rowIdColumnName, string tableAlias, string rowIdColumnNameAlias)> RowIdSelectColumns { get; }

        /// <summary>
        /// A list of aliases for columns in the SELECT statement
        /// </summary>
        IEnumerable<string> SelectColumns { get; }

        /// <summary>
        /// Generate sql. Setup sql will be executd before querySql, but in the same sql script
        /// </summary>
        (string querySetupSql, string querySql) ToSqlString();
    }
}
