using SqlDsl.SqlBuilders;

namespace SqlDsl.MySql
{
    /// <summary>
    /// Class for building mysql statements
    /// </summary>
    public class MySqlBuilder : SqlFragmentBuilderBase
    {
        /// <summary>
        /// Build a sql statement which selects * from a table and adds a unique row id named {rowIdAlias}
        /// </summary>
        public override (string setupSql, string sql) GetSelectTableSqlWithRowId(string tableName, string rowIdAlias)
        {
            var id = "tmp" + GetUniqueId();
            return (
                $"CREATE TEMP TABLE {id} AS SELECT * FROM {WrapTable(tableName)};",
                $"SELECT rowid AS {WrapAlias(rowIdAlias)}, * FROM {WrapTable(id)}");
        }

        /// <summary>
        /// Wrap a table name in parenthesis which protects against illegal characters: []
        /// </summary>
        public override string WrapTable(string table) => $"[{table}]";

        /// <summary>
        /// Wrap a column name in parenthesis which protects against illegal characters: []
        /// </summary>
        public override string WrapColumn(string column) => $"[{column}]";

        /// <summary>
        /// Wrap a table or column alias in parenthesis which protects against illegal characters: []
        /// </summary>
        public override string WrapAlias(string alias) => $"[{alias}]";

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
