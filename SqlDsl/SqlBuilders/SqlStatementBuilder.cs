using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    /// <summary>
    /// 
    /// </summary>
    public class SqlStatementBuilder<TSqlBuilder> : ISqlBuilderOLD
        where TSqlBuilder : ISqlFragmentBuilder, new()
    {
        ISqlFragmentBuilder SqlBuilder;

        public SqlStatementBuilder()
        {
            SqlBuilder = new TSqlBuilder();
        }

        string PrimaryTable;
        public string PrimaryTableAlias { get; private set; }
        public void SetPrimaryTable(string tableName, string alias)
        {
            PrimaryTable = tableName;
            PrimaryTableAlias = alias;
        }

        ISqlBuilderOLD InnerQuery;
        public void SetPrimaryTable(ISqlBuilderOLD table, string alias)
        {
            InnerQuery = table;
            PrimaryTableAlias = alias;
        }
        
        static int _InnerQueryAlias = 0;
        static readonly object InnerQueryLock = new object();
        public string InnerQueryAlias { get; private set; } = BuildInnerQueryAlias();
        static string BuildInnerQueryAlias()
        {
            lock (InnerQueryLock)
            {
                if (_InnerQueryAlias >= 1000)
                    _InnerQueryAlias = 0;

                return $"iq{++_InnerQueryAlias}";
            }
        }















        IEnumerable<(string alias, string sql)> ISqlBuilderOLD.Joins => Joins.Select(x => (x.alias, x.sql));

        readonly List<(string alias, string sql, string setupSql)> Joins = new List<(string, string, string)>();
        public void AddJoin(
            JoinType joinType, 
            string joinTable, 
            ParameterExpression queryRootParam, 
            ParameterExpression joinTableParam,
            Expression equalityStatement, 
            IList<object> paramaters, 
            string joinTableAlias)
        {
            var condition = SqlBuilder.BuildCondition(queryRootParam, new[]{(joinTableParam, joinTableAlias)}, equalityStatement, paramaters);
            var join = BuildJoin(joinType, joinTable, condition.sql, joinTableAlias);

            Joins.Add((
                joinTableAlias, 
                join.sql, 
                new [] { condition.setupSql, join.setupSql }
                    .RemoveNulls()
                    .JoinString(";\n")));
        }

        // /// <summary>
        // /// Build a sql statement which selects * from a table and adds a unique row id named {rowIdAlias}
        // /// </summary>
        // protected abstract string GetUniqueIdSql(string tableName, string rowIdAlias);

        (string setupSql, string sql) BuildJoin(JoinType joinType, string joinTable, string equalityStatement, string joinTableAlias = null)
        {
            joinTableAlias = joinTableAlias == null ? "" : $" {SqlBuilder.WrapAlias(joinTableAlias)}";

            var join = "";
            switch (joinType)
            {
                case JoinType.Inner:
                    join = "INNER";
                    break;
                case JoinType.Left:
                    join = "LEFT";
                    break;
                default:
                    throw new NotImplementedException($"Cannot use join type {joinType}");
            }

            var sql = SqlBuilder.GetUniqueIdSql(joinTable, SqlStatementConstants.RowIdName);
            return (
                sql.setupSql,
                $"{join} JOIN ({sql.sql}){joinTableAlias} ON {equalityStatement}"
            );
        }
        
        public IEnumerable<string> SelectColumns => AllSelectColumns.Select(c => c.alias ?? c.columnName);

        public IEnumerable<(string columnName, string tableName, string alias)> AllSelectColumns => GetAllSelectColumns();

        public IEnumerable<(string columnName, string tableName, string alias)> RowIdSelectColumns => GetRowIdSelectColumns();

        readonly List<(string columnName, string tableName, string alias)> Select = new List<(string columnName, string tableName, string alias)>();
        public void AddSelectColumn(string columnName, string tableName = null, string alias = null) =>
            Select.Add((columnName, tableName, alias));

        public virtual string BuildSelectColumn(string columnName, string tableName = null, string alias = null)
        {
            // TODO: RootObjectAlias should not be used in a virtual method (makes overriding more difficult)
            alias = alias == null || alias.StartsWith($"{SqlStatementConstants.RootObjectAlias}.") ? "" : $" AS {SqlBuilder.WrapAlias(alias)}";

            return tableName == null ? 
                $"{SqlBuilder.WrapColumn(columnName)}{alias}" : 
                $"{SqlBuilder.WrapTable(tableName)}.{SqlBuilder.WrapColumn(columnName)}{alias}";
        }

        IEnumerable<(string columnName, string tableName, string alias)> GetRowIdSelectColumns()
        {
            if (InnerQuery == null)
            {
                foreach (var join in Joins)
                {
                    yield return (SqlStatementConstants.RowIdName, join.alias, $"{join.alias}.{SqlStatementConstants.RowIdName}");
                }

                var ptAlias = PrimaryTableAlias == SqlStatementConstants.RootObjectAlias ? null : $"{PrimaryTableAlias}.{SqlStatementConstants.RowIdName}";
                yield return (SqlStatementConstants.RowIdName, PrimaryTableAlias, ptAlias);
            }
            else
            {
                foreach (var rowId in InnerQuery.RowIdSelectColumns)
                {
                    yield return (rowId.alias ?? rowId.columnName, InnerQuery.InnerQueryAlias, null);
                }
            }
        }

        IEnumerable<(string columnName, string tableName, string alias)> GetAllSelectColumns() =>
            GetRowIdSelectColumns().Concat(Select);

        (string setupSql, string sql)? Where = null;
        public void SetWhere(ParameterExpression queryRoot, Expression equality, IList<object> paramaters)
        {
            Where = SqlBuilder.BuildCondition(queryRoot, Enumerable.Empty<(ParameterExpression, string)>(), equality, paramaters);
        }

        public virtual (string querySetupSql, string querySql) ToSqlString()
        {
            if (PrimaryTable != null && InnerQuery != null)
                throw new InvalidOperationException("You can only call one overload of SetPrimaryTable.");
                
            if (PrimaryTable == null && InnerQuery == null)
                throw new InvalidOperationException("You must call SetPrimaryTable before calling ToSqlString.");
                
            if (PrimaryTableAlias == null)
                throw new InvalidOperationException("You must call SetPrimaryTable before calling ToSqlString.");

            if (!Select.Any())
                throw new InvalidOperationException("You must set at least 1 select column before calling ToSqlString.");

            var innerQuery = InnerQuery?.ToSqlString();

            var setupSql = Joins
                .Select(j => j.setupSql)
                .Concat(new [] 
                {
                    Where?.setupSql,
                    innerQuery?.querySetupSql
                });

            var select = AllSelectColumns
                .Select(s => BuildSelectColumn(s.columnName, s.tableName, s.alias));

            var where = Where == null ? "" : $"WHERE {Where.Value.sql}";

            var primaryTable = innerQuery != null ?
                (null, innerQuery.Value.querySql) :
                SqlBuilder.GetUniqueIdSql(PrimaryTable, SqlStatementConstants.RowIdName);

            setupSql = setupSql.Concat(new [] { primaryTable.setupSql });

            var query = new[]
            {
                $"SELECT {select.JoinString(",")}",
                $"FROM ({primaryTable.sql}) " + SqlBuilder.WrapAlias(PrimaryTableAlias),
                $"{Joins.Select(j => j.sql).JoinString("\n")}",
                $"{where}"
            }
            .Where(x => !string.IsNullOrEmpty(x))
            .JoinString("\n");

            return (
                setupSql
                    .RemoveNulls()
                    .JoinString("\n"), 
                query
            );
        }

        public virtual string TableName(string name, string schema = null)
        {
            schema = schema == null ? "" : $"{SqlBuilder.WrapTable(schema)}.";
            return schema + $"{SqlBuilder.WrapTable(name)}";
        }

        public IEnumerable<(string columnName, string rowIdColumnName)> RowIdMap => 
            InnerQuery != null ?
                GetRowIdMapForInnerQuery() :
                GetRowIdMapForNonInnerQuery();

        public IEnumerable<(string columnName, string rowIdColumnName)> GetRowIdMapForNonInnerQuery()
        {
            foreach (var col in Select)
            {
                foreach (var rid in RowIdSelectColumns)
                {
                    if (col.tableName == rid.tableName)
                    {
                        yield return (col.alias ?? col.columnName, rid.alias ?? rid.columnName);
                        break;
                    }
                }
            }
            
            foreach (var rid in RowIdSelectColumns)
                yield return (rid.alias ?? rid.columnName, rid.alias ?? rid.columnName);
        }

        public IEnumerable<(string columnName, string rowIdColumnName)> GetRowIdMapForInnerQuery()
        {
            var innerMap = InnerQuery.RowIdMap.ToList();
            foreach (var col in Select)
            {
                foreach (var rid in innerMap)
                {
                    if (col.columnName == rid.columnName)
                    {
                        yield return (col.alias ?? col.columnName, rid.rowIdColumnName);
                        break;
                    }
                }
            }
            
            foreach (var rid in innerMap.Select(x => x.rowIdColumnName).Distinct())
                yield return (rid, rid);
        }
    }
}
