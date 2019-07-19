using SqlDsl.Query;
using SqlDsl.Utils;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.SqlBuilders
{
    /// <summary>
    /// Base class for building sql statements. Inherit from this class to build different flavours of sql
    /// </summary>
    public abstract class SqlSyntaxBase : ISqlSyntax
    {
        /// <inheritdoc />
        public virtual string Descending => "DESC";
        
        /// <inheritdoc />
        public virtual string CountFunctionName => "COUNT";
        
        /// <inheritdoc />
        public virtual string SumFunctionName => "SUM";
        
        /// <inheritdoc />
        public virtual string AverageFunctionName => "AVG";
        
        /// <inheritdoc />
        public virtual string MaxFunctionName => "MAX";
        
        /// <inheritdoc />
        public virtual string MinFunctionName => "MIN";

        /// <inheritdoc />
        public virtual string BuildAlias(string lhs, string alias) => $"{lhs} AS {alias}";

        /// <inheritdoc />
        public abstract string WrapTable(string table);

        /// <inheritdoc />
        public abstract string WrapColumn(string column);

        /// <inheritdoc />
        public abstract string WrapAlias(string alias);

        /// <inheritdoc />
        public virtual string BuildAndCondition(string lhs, string rhs) => $"{lhs} AND {rhs}";

        /// <inheritdoc />
        public virtual string BuildOrCondition(string lhs, string rhs) => $"{lhs} OR {rhs}";

        /// <inheritdoc />
        public virtual string BuildEqualityCondition(string lhs, string rhs) => $"{lhs} = {rhs}";

        /// <inheritdoc />
        public virtual string BuildNonEqualityCondition(string lhs, string rhs) => $"{lhs} <> {rhs}";

        /// <inheritdoc />
        public virtual string BuildLessThanCondition(string lhs, string rhs) => $"{lhs} < {rhs}";

        /// <inheritdoc />
        public virtual string BuildLessThanEqualToCondition(string lhs, string rhs) => $"{lhs} <= {rhs}";

        /// <inheritdoc />
        public virtual string BuildGreaterThanCondition(string lhs, string rhs) => $"{lhs} > {rhs}";

        /// <inheritdoc />
        public virtual string BuildGreaterThanEqualToCondition(string lhs, string rhs) => $"{lhs} >= {rhs}";

        /// <inheritdoc />
        public virtual string BuildInCondition(string lhs, string rhs)
        {
            // todo: not ideal
            if (rhs.Length > 0 && rhs[0] != '(')
                rhs = $"({rhs})";

            return $"{lhs} IN {rhs}";
        }

        /// <inheritdoc />
        public virtual string BuildCommaCondition(string lhs, string rhs) => $"{lhs}, {rhs}";

        /// <inheritdoc />
        public virtual string BuildAddCondition(string lhs, string rhs) => $"{lhs} + {rhs}";

        /// <inheritdoc />
        public virtual string BuildSubtractCondition(string lhs, string rhs) => $"{lhs} - {rhs}";

        /// <inheritdoc />
        public virtual string BuildMultiplyCondition(string lhs, string rhs) => $"({lhs} * {rhs})";

        /// <inheritdoc />
        public virtual string BuildDivideCondition(string lhs, string rhs) => $"{lhs} / {rhs}";

        // TODO: other operators (e.g. OR)

        /// <inheritdoc />
        public virtual (string setupSql, string sql) AddDenseRank(IEnumerable<(string sql, string columnAlias)> selectColumns, string denseRankAlias, IEnumerable<(string, OrderDirection)> orderByClauses, string restOfQuery)
        {
            var denseRank = orderByClauses
                .Select(AddOrdering)
                .Aggregate(BuildCommaCondition);
                
            var selectCols = selectColumns
                .Select(x => x.sql)
                .Append(BuildAlias($"DENSE_RANK() OVER (ORDER BY {denseRank})", WrapAlias(denseRankAlias)))
                .Aggregate(BuildCommaCondition);

            return (null, $"SELECT {selectCols}\n{restOfQuery}");

            string AddOrdering((string, OrderDirection) p) => p.Item2 == OrderDirection.Descending 
                ? $"{p.Item1} {Descending}"
                : p.Item1; 
        }

        /// <inheritdoc />
        public virtual SelectTableSqlWithRowId GetSelectTableSqlWithRowId(string tableName, string rowIdAlias, IEnumerable<string> otherColumnNames)
        {
            var oc = otherColumnNames.ToList();
            var cols = oc
                .Select(WrapColumn)
                // TODO: rownumber over columns should not be first column, but rather primary key
                .Prepend(BuildAlias($"(ROW_NUMBER() OVER (ORDER BY {WrapColumn(oc[0])}))", WrapAlias(rowIdAlias)))
                .JoinString(",");

            return new SelectTableSqlWithRowId(
                null,
                $"SELECT {cols} FROM {WrapTable(tableName)}",
                null,
                false);
        }

        /// <inheritdoc />
        public virtual SelectTableSqlWithRowId GetSelectTableSqlWithPrimaryKey(string tableName, IEnumerable<(string col, string alias)> primaryKeyColumns, IEnumerable<string> otherColumnNames)
        {
            var cols = primaryKeyColumns
                .Select(x => BuildAlias(WrapColumn(x.col), WrapAlias(x.alias)))
                .Concat(otherColumnNames
                    .Select(WrapColumn))
                .JoinString(",");

            return new SelectTableSqlWithRowId(
                null,
                $"SELECT {cols} FROM {WrapTable(tableName)}",
                null,
                false);
        }
    }
}
