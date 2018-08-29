using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.SqlBuilders;

namespace SqlDsl.Sqlite
{
    /// <summary>
    /// Class for building sqlite statements
    /// </summary>
    public class SqliteBuilder : SqlBuilderBase
    {
        /// <summary>
        /// Build a sql statement which selects * from a table and adds a unique row id named {rowIdAlias}
        /// </summary>
        protected override string GetUniqueIdSql(string tableName, string rowIdAlias)
        {
            var id = GetUniqueId();

            AddStatementBefore($"CREATE TEMP TABLE tmp{id} AS SELECT * FROM {WrapTable(tableName)};");
            return $"SELECT rowid AS {WrapAlias(rowIdAlias)}, * FROM {WrapTable("tmp" + id)}";
        }

        /// <summary>
        /// Wrap a table name in parenthesis whcih protect against illegal characters: []
        /// </summary>
        protected override string WrapTable(string table) => $"[{table}]";

        /// <summary>
        /// Wrap a column name in parenthesis whcih protect against illegal characters: []
        /// </summary>
        protected override string WrapColumn(string column) => $"[{column}]";

        /// <summary>
        /// Wrap a table or column alias in parenthesis whcih protect against illegal characters: []
        /// </summary>
        protected override string WrapAlias(string alias) => $"[{alias}]";

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
