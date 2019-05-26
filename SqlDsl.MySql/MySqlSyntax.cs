using System.Collections.Generic;
using System.Linq;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.MySql
{
    /// <summary>
    /// Class for building MySql statements
    /// </summary>
    public class MySqlSyntax : SqlSyntaxBase
    {
        /// <summary>
        /// Build a sql statement which selects * from a table and adds a unique row id named {rowIdAlias}
        /// </summary>
        public override SelectTableSqlWithRowId GetSelectTableSqlWithRowId(string tableName, string rowIdAlias, IEnumerable<string> otherColumnNames)
        {
            var id = "row_number" + GetUniqueId();
            var cols = otherColumnNames
                .Select(WrapColumn)
                .Prepend($"(@{id}:=@{id} + 1) AS {WrapAlias(rowIdAlias)}")
                .JoinString(",");

            return new SelectTableSqlWithRowId(
                $"SET @{id} = 0;",
                $"SELECT {cols} FROM {WrapTable(tableName)}",
                null,
                false);
        }

        /// <summary>
        /// Wrap a table name in parenthesis which protects against illegal characters: []
        /// </summary>
        public override string WrapTable(string table) => $"`{table}`";

        /// <summary>
        /// Wrap a column name in parenthesis which protects against illegal characters: []
        /// </summary>
        public override string WrapColumn(string column) => $"`{column}`";

        /// <summary>
        /// Wrap a table or column alias in parenthesis which protects against illegal characters: []
        /// </summary>
        public override string WrapAlias(string alias) => $"`{alias}`";

        static readonly object Lock = new object();
        static int TmpIdentifier = 0;
        
        /// <summary>
        /// Get a unique id which can be used in the context of the running application
        /// </summary>
        static int GetUniqueId()
        {
            lock (Lock)
                return ++TmpIdentifier;
        }
    }
}
