using SqlDsl.Mapper;
using SqlDsl.Query;
using SqlDsl.SqlBuilders.SqlStatementParts;
using SqlDsl.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    /// <summary>
    /// A class to build sql statements
    /// </summary>
    public class MappedSqlStatementBuilder : SqlStatementBuilderBase, SqlStatementParts.ISqlStatementPartValues
    {
        /// <summary>
        /// The inner query used in the SELECT clause
        /// </summary>
        readonly ISqlString InnerQueryBuilder;
        
        /// <summary>
        /// The inner query used in the SELECT clause
        /// </summary>
        readonly ISqlStatement InnerQueryStatement;

        /// <summary>
        /// A map from a row id column to a location in a mapped property graph
        /// </summary>
        public readonly List<(string rowIdColumnName, string resultClassProperty)> RowIdsForMappedProperties = new List<(string, string)>();

        public MappedSqlStatementBuilder(ISqlSyntax sqlFragmentBuilder, ISqlString innerQueryBuilder, ISqlStatement innerQueryStatement, string primaryTableAlias)
            : base(sqlFragmentBuilder, primaryTableAlias)
        {
            InnerQueryBuilder = innerQueryBuilder ?? throw new ArgumentNullException(nameof(innerQueryBuilder));
            InnerQueryStatement = innerQueryStatement ?? throw new ArgumentNullException(nameof(innerQueryStatement));
        }

        /// <inheritdoc />
        protected override (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) ToSqlString(IEnumerable<string> selectColumns)
        {
            // get the sql from the inner query if possible
            var (querySetupSql, beforeWhereSql, whereSql, afterWhereSql) = InnerQueryBuilder.ToSqlString();

            beforeWhereSql = $"SELECT {selectColumns.JoinString(",")}\nFROM ({beforeWhereSql}";
            afterWhereSql = $"{afterWhereSql}) {SqlBuilder.WrapAlias(PrimaryTableAlias)}{BuildGroupByStatement("\n")}";

            return (querySetupSql, beforeWhereSql, whereSql, afterWhereSql);
        }

        string BuildGroupByStatement(string prefix)
        {
            if (GetAllSelectColumns().All(cs => cs.col.RepresentsColumns.All(c => c.aggregatedToTable == null)))
                return "";

            var output = new List<string>(16);
            foreach (var col in GetAllSelectColumns()
                .SelectMany(cs => cs.col.RepresentsColumns)
                .Where(c => c.aggregatedToTable == null))
            {
                output.Add(SqlBuilder.BuildSelectColumn(col.table, col.column));
            }

            if (output.Count > 0)
                prefix += "GROUP BY ";

            return $"{prefix}{output.JoinString(",")}";
        }

        /// <inheritdoc />
        protected override IEnumerable<(string rowIdColumnName, string tableAlias, string rowIdColumnNameAlias)> GetRowIdSelectColumns()
        {
            // if there is an inner query, all columns will come from it
            foreach (var table in InnerQueryStatement.Tables)
            {
                // Get row id from the SELECT
                var alias = table.Alias == null || table.Alias == SqlStatementConstants.RootObjectAlias ? 
                    SqlStatementConstants.RowIdName : 
                    $"{table.Alias}.{SqlStatementConstants.RowIdName}";
                
                // the only row id will be [inner query alias].[##rowid]
                yield return (
                    InnerQueryStatement.SelectColumns[table.RowNumberColumnIndex].Alias, 
                    InnerQueryStatement.UniqueAlias, 
                    alias);
            }
        }
        
        #region ISqlStatementPartValues

        string ISqlStatementPartValues.UniqueAlias => null;

        string ISqlStatementPartValues.PrimaryTableAlias => PrimaryTableAlias;

        IEnumerable<SqlStatementPartJoin> ISqlStatementPartValues.JoinTables => Enumerable.Empty<SqlStatementPartJoin>();

        ISqlStatement ISqlStatementPartValues.InnerStatement => InnerQueryStatement;

        ISqlSyntax ISqlStatementPartValues.SqlBuilder => SqlBuilder;

        IEnumerable<SqlStatementPartSelect> ISqlStatementPartValues.SelectColumns => GetAllSelectColumns().Select(BuildSelectCol);

        IEnumerable<(string rowIdColumnName, string resultClassProperty)> ISqlStatementPartValues.RowIdsForMappedProperties => RowIdsForMappedProperties;

        static readonly Func<(string alias, string sql, string setupSql, IEnumerable<string> queryObjectReferences), SqlStatementPartJoin> BuildJoinTable = join =>
            new SqlStatementPartJoin(join.alias, join.queryObjectReferences);

        static readonly Func<(bool, SelectColumn), SqlStatementPartSelect> BuildSelectCol = select =>
            // TODO: string manipulation
            new SqlStatementPartSelect(select.Item1, select.Item2.CellDataType, select.Item2.Alias, select.Item2.RepresentsColumns, select.Item2.ArgConstructors);

        #endregion
    }
}