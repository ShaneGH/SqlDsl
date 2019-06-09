using System.Collections.Generic;
using System.Linq;
using SqlDsl.Query;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.Sqlite
{
    /// <summary>
    /// Class for building sqlite statements
    /// </summary>
    public class SqliteSyntax : SqlSyntaxBase
    {
        /// <summary>
        /// Build a sql statement which selects * from a table and adds a unique row id named {rowIdAlias}
        /// </summary>
        public override SelectTableSqlWithRowId GetSelectTableSqlWithRowId(string tableName, string rowIdAlias, IEnumerable<string> otherColumnNames)
        {
            var id = "tmp" + GetUniqueId();
            var cols = otherColumnNames
                .Select(WrapColumn)
                .Prepend($"rowid AS {WrapAlias(rowIdAlias)}")
                .JoinString(",");

            return new SelectTableSqlWithRowId(
                $"CREATE TEMP TABLE {id} AS SELECT * FROM {WrapTable(tableName)};",
                $"SELECT {cols} FROM {WrapTable(id)}",
                $"DROP TABLE {id};",
                false);
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

        /// <inheritdoc />
        public override (string setupSql, string sql) AddDenseRank(IEnumerable<string> selectColumns, string denseRankAlias, IEnumerable<(string, OrderDirection)> orderByClauses, string restOfQuery)
        {
            throw new System.NotImplementedException();
        }

        static readonly object Lock = new object();
        static int TmpIdentifier = 0;
        
        /// <summary>
        /// Get a unique id which can be used in the context of the running application
        /// </summary>
        static int GetUniqueId()
        {
            lock (Lock)
            {
                if (TmpIdentifier > 1000000)
                    TmpIdentifier = 0;

                return ++TmpIdentifier;
            }
        }
    }
}
