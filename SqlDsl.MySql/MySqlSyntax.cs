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
                ? base.GetSelectTableSqlWithRowId(tableName, rowIdAlias, otherColumnNames)
                : GetSelectTableSqlWithRowIdV7(tableName, rowIdAlias, otherColumnNames);
        }

        SelectTableSqlWithRowId GetSelectTableSqlWithRowIdV7(string tableName, string rowIdAlias, IEnumerable<string> otherColumnNames)
        {
            var id = "row_number" + GetUniqueId();
            var cols = otherColumnNames
                .Select(WrapColumn)
                .Prepend($"(@{id}:=@{id}+1) AS {WrapAlias(rowIdAlias)}")
                .JoinString(",");

            return new SelectTableSqlWithRowId(
                $"SET @{id}=0;",
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
        public override (string setupSql, string sql) AddDenseRank(IEnumerable<(string sql, string columnAlias)> selectColumns, string denseRankAlias, IEnumerable<(string, OrderDirection)> orderByClauses, string restOfQuery)
        {
            return Settings.Version8OrHigher
                ? base.AddDenseRank(selectColumns, denseRankAlias, orderByClauses, restOfQuery)
                : AddDenseRankV7(selectColumns, denseRankAlias, orderByClauses, restOfQuery);
        }

        /// <summary>
        /// Convert an order by clause from an inner statement into one from an outer statement
        /// </summary>
        IEnumerable<(OrderDirection direction, string orderByAlias, string orderBySelectSql)> BuildOrderByAliases(IEnumerable<(string sql, string columnAlias)> selectColumns, IEnumerable<(string sql , OrderDirection direction)> orderByClauses)
        {
            var i = 0;
            selectColumns = selectColumns.Enumerate();
            foreach (var orderBy in orderByClauses)
            {
                var col = selectColumns
                    .AsNullable()
                    .FirstOrDefault(c => c.Value.sql == orderBy.sql);

                if (col != null)
                {
                    yield return (
                        orderBy.direction,
                        col.Value.columnAlias,
                        null);
                }
                else
                {
                    var alias = $"{SqlStatementConstants.OrderByAlias}{++i}";
                    yield return (
                        orderBy.direction,
                        alias,
                        orderBy.sql);
                }
            }
        }

        (string setupSql, string sql) AddDenseRankV7(IEnumerable<(string sql, string columnAlias)> selectColumns, string denseRankAlias, IEnumerable<(string, OrderDirection)> orderByClauses, string restOfQuery)
        {
            selectColumns = selectColumns.Enumerate();
            var orderByWithAlias = BuildOrderByAliases(selectColumns, orderByClauses).Enumerate();

            var orderBy = orderByWithAlias
                .Select(AddOrdering)
                .Aggregate(BuildCommaCondition);

            var rowId = "@drp" + GetUniqueId();
            var drParams = orderByWithAlias
                .Select(_ => "@drp" + GetUniqueId())
                .ToList();

            var drCondition = orderByWithAlias
                .Select((c, i) => BuildAndCondition($"{drParams[i]}=({drParams[i]}:={WrapAlias(c.orderByAlias)})", $"{rowId}<>0"))
                .Aggregate(BuildAndCondition);

            var drSelect = BuildAlias($"IF({drCondition},{rowId},{rowId}:={rowId}+1)", WrapAlias(denseRankAlias));

            var orderBySels = orderByWithAlias
                .Where(x => x.orderBySelectSql != null)
                .Select(x => BuildAlias(x.orderBySelectSql, WrapAlias(x.orderByAlias)));

            var innerSelCols = selectColumns
                .Select(x => x.sql)
                .Concat(orderBySels)
                .Aggregate(BuildCommaCondition);

            var outerSelCols = BuildCommaCondition("*", drSelect);

            var originalQuery = $"SELECT {innerSelCols}\n{restOfQuery}\nORDER BY {orderBy}";

            return (
                // p might not be an int data type, mysql is able to switch types.
                // for some reason it cannot switch types if SET p = NULL
                drParams.Select(p => $"SET {p} = 0;").Prepend($"SET {rowId} = 0;").JoinString("\n"),
                $"SELECT {outerSelCols} FROM (\n{originalQuery}) {WrappedInnerQuery}");

            string AddOrdering((OrderDirection, string alias, string sql) p) => p.Item1 == OrderDirection.Descending 
                ? $"{WrapAlias(p.alias)} {Descending}"
                : WrapAlias(p.alias); 
        }

        string _WrappedInnerQuery;
        string WrappedInnerQuery => _WrappedInnerQuery ?? (_WrappedInnerQuery = WrapAlias(SqlDsl.SqlBuilders.SqlStatementConstants.InnerQueryAlias));
    }
}
