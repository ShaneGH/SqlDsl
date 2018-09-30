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
    /// A class to build sql statements
    /// </summary>
    public class SqlStatementBuilder<TSqlBuilder> : ISqlStatement
        where TSqlBuilder : ISqlFragmentBuilder, new()
    {
        #region ISqlStatement

        IEnumerable<string> ISqlStatement.SelectColumns
             => GetAllSelectColumns().Select(c => c.alias ?? c.columnName);
             
        public IEnumerable<(string rowIdColumnName, string tableAlias, string rowIdColumnNameAlias)> RowIdSelectColumns
             => GetRowIdSelectColumns();

        IEnumerable<(string columnName, string rowIdColumnName)> ISqlStatement.RowIdMap => GetRowIdMap();
                
        public string UniqueAlias { get; private set; } = BuildInnerQueryAlias();

        IEnumerable<(string rowIdColumnName, string resultClassProperty)> ISqlStatement.RowIdsForMappedProperties => RowIdsForMappedProperties.Skip(0);

        IEnumerable<(string from, string to)> ISqlStatement.JoinedTables => GetJoinedTables();

        #endregion

        ISqlFragmentBuilder SqlBuilder;

        public SqlStatementBuilder()
        {
            SqlBuilder = new TSqlBuilder();
        }

        /// <summary>
        /// The name of the table in the SELECT clause
        /// </summary>
        string PrimaryTable;
        
        /// <summary>
        /// The alias of the table in the SELECT clause
        /// </summary>
        public string PrimaryTableAlias { get; private set; }
        
        /// <summary>
        /// Set the name and alias of the table in the SELECT clause. alias can be null
        /// </summary>
        public void SetPrimaryTable(string tableName, string alias)
        {
            PrimaryTable = tableName;
            PrimaryTableAlias = alias;
        }

        /// <summary>
        /// The inner query used in the SELECT clause
        /// </summary>
        ISqlStatement InnerQuery;
        
        /// <summary>
        /// Set the inner query and is's alias in the SELECT clause. alias can be null
        /// </summary>
        public void SetPrimaryTable(ISqlStatement table, string alias)
        {
            InnerQuery = table;
            PrimaryTableAlias = alias;
        }
        
        static int _InnerQueryAlias = 0;
        static readonly object InnerQueryLock = new object();
        
        /// <summary>
        /// Get a new 4 digit code for alias. Aliass' must be unique in the scope of a query only
        /// </summary>
        static string BuildInnerQueryAlias()
        {
            lock (InnerQueryLock)
            {
                if (_InnerQueryAlias >= 10000)
                    _InnerQueryAlias = 0;

                return $"iq{++_InnerQueryAlias}";
            }
        }

        /// <summary>
        /// A map from a row id column to a location in a mapped property graph
        /// </summary>
        public readonly List<(string rowIdColumnName, string resultClassProperty)> RowIdsForMappedProperties = new List<(string, string)>();

        /// <summary>
        /// A list of joins including their name, sql and any sql which must be run before the query to facilitate the join
        /// </summary>
        readonly List<(string alias, string sql, string setupSql, IEnumerable<string> queryObjectReferences)> Joins = new List<(string, string, string, IEnumerable<string>)>();
        
        /// <summary>
        /// Add a JOIN to the query
        /// </summary>
        /// <param name="joinType">The type, e.g. INNER, LEFT etc...</param>
        /// <param name="joinTable">The table to join on</param>
        /// <param name="queryRootParam">The parameter which represents the query object in the expression</param>
        /// <param name="joinTableParam">The parameter which represents the join table in the expression</param>
        /// <param name="equalityStatement">The ON part of the join</param>
        /// <param name="parameters">A list of parameters which will be added to if a constant is found in the equalityStatement</param>
        /// <param name="joinTableAlias">The alias of the join statement</param>
        public void AddJoin(
            JoinType joinType, 
            string joinTable, 
            ParameterExpression queryRootParam, 
            ParameterExpression queryArgsParam,
            ParameterExpression joinTableParam,
            Expression equalityStatement, 
            IList<object> parameters, 
            string joinTableAlias)
        {
            var condition = SqlBuilder.BuildCondition(
                queryRootParam, 
                queryArgsParam,
                new[]{ (joinTableParam, joinTableAlias) }, 
                equalityStatement, 
                parameters);

            var join = BuildJoin(joinType, joinTable, condition.sql, joinTableAlias);

            Joins.Add((
                joinTableAlias, 
                join.sql, 
                // combine all setup sql statements
                new [] { condition.setupSql, join.setupSql }
                    .RemoveNullOrEmpty()
                    .JoinString(";\n"),
                condition.queryObjectReferences));
        }

        /// <summary>
        /// Build JOIN sql
        /// </summary>
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

            var sql = SqlBuilder.GetSelectTableSqlWithRowId(joinTable, SqlStatementConstants.RowIdName);
            return (
                sql.setupSql,
                $"{join} JOIN ({sql.sql}){joinTableAlias} ON {equalityStatement}"
            );
        }

        /// <summary>
        /// A list of columns in the SELECT statement
        /// </summary>
        readonly List<(string columnName, string tableName, string alias)> Select = new List<(string columnName, string tableName, string alias)>();
        
        /// <summary>
        /// Add a column to the SELECT statement
        /// </summary>
        public void AddSelectColumn(string columnName, string tableName = null, string alias = null) =>
            Select.Add((columnName, tableName, alias));

        /// <summary>
        /// The WHERE statement, if necessary
        /// </summary>
        (string setupSql, string sql, IEnumerable<string> queryObjectReferences)? Where = null;
        
        /// <summary>
        /// The WHERE statement, if necessary
        /// </summary>
        /// <param name="queryRoot">The parameter which represents the query object in the expression</param>
        /// <param name="args">The parameter which represents the query args in the expression</param>
        /// <param name="equality">The condition in the WHERE statement</param>
        /// <param name="parameters">A list of parameters which will be added to if a constant is found in the equalityStatement</param>
        public void SetWhere(ParameterExpression queryRoot, ParameterExpression args, Expression equality, IList<object> parameters)
        {
            Where = SqlBuilder.BuildCondition(queryRoot, args, Enumerable.Empty<(ParameterExpression, string)>(), equality, parameters);
        }

        /// <summary>
        /// Compile the sql statment to a script
        /// </summary>
        /// <returns>querySetupSql: sql which must be executed before the query is run. querySql: the query sql</returns>
        public (string querySetupSql, string querySql) ToSqlString()
        {
            if (PrimaryTable != null && InnerQuery != null)
                throw new InvalidOperationException("You can only call one overload of SetPrimaryTable.");
                
            if (PrimaryTable == null && InnerQuery == null)
                throw new InvalidOperationException("You must call SetPrimaryTable before calling ToSqlString.");
                
            if (PrimaryTableAlias == null)
                throw new InvalidOperationException("You must call SetPrimaryTable before calling ToSqlString.");

            if (!Select.Any())
                throw new InvalidOperationException("You must set at least 1 select column before calling ToSqlString.");

            // get the sql from the inner query if possible
            var innerQuery = InnerQuery?.ToSqlString();

            // build SELECT columns (cols and row ids)
            var select = GetAllSelectColumns()
                .Select(s => BuildSelectColumn(s.columnName, s.tableName, s.alias));

            // build WHERE part
            var where = Where == null ? "" : $"WHERE {Where.Value.sql}";

            // build FROM part
            var primaryTable = innerQuery != null ?
                (null, innerQuery.Value.querySql) :
                SqlBuilder.GetSelectTableSqlWithRowId(PrimaryTable, SqlStatementConstants.RowIdName);
                
            // concat all setup sql from all other parts
            var setupSql = Joins
                .Select(j => j.setupSql)
                .Concat(new [] 
                {
                    Where?.setupSql,
                    innerQuery?.querySetupSql,
                    primaryTable.setupSql
                })
                .RemoveNulls()
                .JoinString("\n");

            var query = new[]
            {
                $"SELECT {select.JoinString(",")}",
                $"FROM ({primaryTable.sql}) " + SqlBuilder.WrapAlias(PrimaryTableAlias),
                $"{Joins.Select(j => j.sql).JoinString("\n")}",
                $"{where}"
            }
            .Where(x => !string.IsNullOrEmpty(x))
            .JoinString("\n");

            return (setupSql, query);
        }

        /// <summary>
        /// Build the string for a SELECT column
        /// </summary>
        string BuildSelectColumn(string columnName, string tableName = null, string alias = null)
        {
            alias = alias == null || alias.StartsWith($"{SqlStatementConstants.RootObjectAlias}.") ? "" : $" AS {SqlBuilder.WrapAlias(alias)}";

            return tableName == null ? 
                $"{SqlBuilder.WrapColumn(columnName)}{alias}" : 
                $"{SqlBuilder.WrapTable(tableName)}.{SqlBuilder.WrapColumn(columnName)}{alias}";
        }

        /// <summary>
        /// Get a list of row id colums, the alias of the table they are identifying, and the alias for the row id column (if any)
        /// </summary>
        IEnumerable<(string rowIdColumnName, string tableAlias, string rowIdColumnNameAlias)> GetRowIdSelectColumns()
        {
            // if there is no inner query, columns will come from SELECT and JOIN parts
            if (InnerQuery == null)
            {
                // Get row id from the SELECT
                var ptAlias = PrimaryTableAlias == SqlStatementConstants.RootObjectAlias ? 
                    null : 
                    $"{PrimaryTableAlias}.{SqlStatementConstants.RowIdName}";

                yield return (SqlStatementConstants.RowIdName, PrimaryTableAlias, ptAlias);

                // Get row id from each join
                foreach (var join in Joins)
                {
                    yield return (SqlStatementConstants.RowIdName, join.alias, $"{join.alias}.{SqlStatementConstants.RowIdName}");
                }
            }
            else
            {
                // if there is an inner query, all columns will come from it
                foreach (var rowId in InnerQuery.RowIdSelectColumns)
                {
                    // the only row id will be [inner query alias].[##rowid]
                    yield return (rowId.rowIdColumnNameAlias ?? rowId.rowIdColumnName, InnerQuery.UniqueAlias, null);
                }
            }
        }

        /// <summary>
        /// Concat DB table columns with row id columns
        /// </summary>
        IEnumerable<(string columnName, string tableName, string alias)> GetAllSelectColumns() =>
            GetRowIdSelectColumns().Concat(Select); // TODO: should be Select.Concat(GetRowIdSelectColumns())

        IEnumerable<(string columnName, string rowIdColumnName)> GetRowIdMap() => InnerQuery != null ?
            GetRowIdMapForInnerQuery() :
            GetRowIdMapForNonInnerQuery();

        /// <summary>
        /// Get a map of all columns to their respective row id column, if InnerQuery == null
        /// </summary>
        IEnumerable<(string columnName, string rowIdColumnName)> GetRowIdMapForNonInnerQuery()
        {
            // get a row id for each SELECT column
            foreach (var col in Select)
            {
                foreach (var rid in RowIdSelectColumns)
                {
                    if (col.tableName == rid.tableAlias)
                    {
                        yield return (col.alias ?? col.columnName, rid.rowIdColumnNameAlias ?? rid.rowIdColumnName);
                        break;
                    }
                }
            }
            
            // foreach row id, return a reference to itself
            foreach (var rid in RowIdSelectColumns)
                yield return (rid.rowIdColumnNameAlias ?? rid.rowIdColumnName, rid.rowIdColumnNameAlias ?? rid.rowIdColumnName);
        }

        /// <summary>
        /// Get a map of all columns to their respective row id column, if InnerQuery != null
        /// </summary>
        IEnumerable<(string columnName, string rowIdColumnName)> GetRowIdMapForInnerQuery()
        {
            // get the map fron the inner query, and 
            // map the inner query columns to the outer query ones
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
            
            // foreach row id, return a reference to itself
            foreach (var rid in innerMap.Select(x => x.rowIdColumnName).Distinct())
                yield return (rid, rid);
        }

        static readonly IEnumerable<int> EmptyInts = Enumerable.Empty<int>();

        /// <summary>
        /// Get a list of tables which have a join to one another
        /// </summary>
        IEnumerable<(string from, string to)> GetJoinedTables()
        {
            // TDO: hacky
            // TODO; unify join table property and row id col numbers
            return Joins
                .SelectMany(j => j.queryObjectReferences.Select(o => (j.alias, o)))
                .Distinct();
        }

        /// <summary>
        /// Given a row id column index, return the column index for the row id of the table it needs to join on. Null means that the table has no dependant joins
        /// </summary>
        public int? GetDependantRowId(int rowIdColumnIndex)
        {
            if (InnerQuery != null)
                return InnerQuery.GetDependantRowId(rowIdColumnIndex);

            // 0 rowid means the primary table
            if (rowIdColumnIndex == 0) return null;

            // account for primary table being first in list
            var join = Joins[rowIdColumnIndex - 1];
            if (join.queryObjectReferences.Count() == 0)
                return null;

            if (join.queryObjectReferences.Count() > 1)
                // TODO
                throw new NotImplementedException("Cannot support joins on 2 seperate tables at the moment");
                
            var tableName = join.queryObjectReferences.First();
            if (PrimaryTableAlias == tableName)
                return 0;

            for (var i = 0; i < Joins.Count; i++)
            {
                if (Joins[i].alias == tableName)
                    return i + 1;
            }
            
            throw new InvalidOperationException(
                $"Cannot find row id index for table alias \"{tableName}\". " + 
                $"Have you added a Join to the \"{tableName}\" property of the query object?");
        }

        /// <summary>
        /// Given a row id column index, return a chain of column indexes back to the root for the row id of the table it needs to join on.
        /// </summary>
        public IEnumerable<int> GetDependantRowIdChain(int rowId)
        {
            int? rid = rowId;
            var result = new List<int>();
            while (rid != null)
            {
                result.Insert(0, rid.Value);
                rid = GetDependantRowId(rid.Value);
            }

            return result.Skip(0);
        }
    }
}