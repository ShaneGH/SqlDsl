using System;
using System.Collections.Generic;
using System.Linq;
using SqlDsl.Query;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.MySql
{
    /// <summary>
    /// Class for building MySql statements
    /// </summary>
    public class MySqlSyntax : SqlSyntaxBase
    {
        readonly MySqlSettings Settings;

        public MySqlSyntax(MySqlSettings settings)
        {
            Settings = settings ?? throw new ArgumentNullException(nameof(settings));
        }

        /// <summary>
        /// Build a sql statement which selects * from a table and adds a unique row id named {rowIdAlias}
        /// </summary>
        public override SelectTableSqlWithRowId GetSelectTableSqlWithRowId(string tableName, string rowIdAlias, IEnumerable<string> otherColumnNames)
        {
            return Settings.Version8OrHigher
                ? GetSelectTableSqlWithRowIdV8(tableName, rowIdAlias, otherColumnNames)
                : GetSelectTableSqlWithRowIdV7(tableName, rowIdAlias, otherColumnNames);
        }

        SelectTableSqlWithRowId GetSelectTableSqlWithRowIdV7(string tableName, string rowIdAlias, IEnumerable<string> otherColumnNames)
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

        SelectTableSqlWithRowId GetSelectTableSqlWithRowIdV8(string tableName, string rowIdAlias, IEnumerable<string> otherColumnNames)
        {
            var oc = otherColumnNames.ToList();
            var cols = otherColumnNames
                .Select(WrapColumn)
                // TODO: rownumber over columns should not be first, but rather primary key
                .Prepend($"(ROW_NUMBER() OVER (ORDER BY {WrapColumn(oc[0])})) AS {WrapAlias(rowIdAlias)}")
                .JoinString(",");

            return new SelectTableSqlWithRowId(
                null,
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
            {
                if (TmpIdentifier > 1000000)
                    TmpIdentifier = 0;

                return ++TmpIdentifier;
            }
        }

        /// <inheritdoc />
        public override string AddDenseRank(IEnumerable<string> selectColumns, string denseRankAlias, IEnumerable<(string, OrderDirection)> orderByClauses, string restOfQuery)
        {
            var denseRank = orderByClauses
                .Select(AddOrdering)
                .Aggregate(BuildCommaCondition);
                
            var selectCols = selectColumns
                .Append($"DENSE_RANK() OVER (ORDER BY {denseRank}) AS {WrapAlias(denseRankAlias)}")
                .Aggregate(BuildCommaCondition);

            return $"SELECT {selectCols}\n{restOfQuery}";

            string AddOrdering((string, OrderDirection) p) => p.Item2 == OrderDirection.Descending 
                ? $"{p.Item1} {Descending}"
                : p.Item1; 
        }
    }
}
