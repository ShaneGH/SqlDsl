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
        /// A map from a row id column to a location in a mapped property graph
        /// </summary>
        IEnumerable<(string rowIdColumnName, string resultClassProperty)> RowIdsForMappedProperties { get; }

        /// <summary>
        /// A list of row id colums, the alias of the table they are identifying, and the alias for the row id column (if any)
        /// </summary>
        IEnumerable<(string rowIdColumnName, string tableAlias, string rowIdColumnNameAlias)> RowIdSelectColumns { get; }

        /// <summary>
        /// A list of aliases for columns in the SELECT statement
        /// </summary>
        IEnumerable<string> SelectColumns { get; }

        /// <summary>
        /// A list of tables which have a join to one another
        /// </summary>
        IEnumerable<(string from, string to)> JoinedTables { get; }

        /// <summary>
        /// Generate sql. Setup sql will be executd before querySql, but in the same sql script
        /// </summary>
        (string querySetupSql, string querySql) ToSqlString();

        /// <summary>
        /// Given a row id column index, return the column index for the row id of the table it needs to join on. Null means that the table has no dependant joins
        /// </summary>
        int? GetDependantRowId(int rowId);

        /// <summary>
        /// Given a row id column index, return a chain of column indexes back to the root for the row id of the table it needs to join on.
        /// </summary>
        IEnumerable<int> GetDependantRowIdChain(int rowId);
    }
}
