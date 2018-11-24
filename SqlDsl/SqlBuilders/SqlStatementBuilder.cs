using SqlDsl.Query;
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
    public class SqlStatementBuilder<TSqlBuilder> : SqlStatementBuilder
        where TSqlBuilder : ISqlFragmentBuilder, new()
    {
        public SqlStatementBuilder() : base(new TSqlBuilder()) { }
    }

    /// <summary>
    /// A class to build sql statements
    /// </summary>
    public class SqlStatementBuilder : ISqlBuilder
    {
        readonly ISqlFragmentBuilder SqlBuilder;

        public SqlStatementBuilder(ISqlFragmentBuilder sqlFragmentBuilder)
        {
            SqlBuilder = sqlFragmentBuilder ?? throw new ArgumentNullException(nameof(sqlFragmentBuilder));
        }
                
        public readonly string UniqueAlias = BuildInnerQueryAlias();

        /// <summary>
        /// The name of the table in the SELECT clause
        /// </summary>
        string PrimaryTable;
        
        /// <summary>
        /// The alias of the table in the SELECT clause
        /// </summary>
        public string PrimaryTableAlias;
        
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
        (ISqlBuilder builder, ISqlStatement statement)? InnerQuery;

        /// <summary>
        /// The inner statement used in the SELECT clause, or null if there is no inner statement
        /// </summary>
        public ISqlStatement InnerStatement => InnerQuery?.statement;
        
        /// <summary>
        /// Set the inner query and is's alias in the SELECT clause. alias can be null
        /// </summary>
        public void SetPrimaryTable(ISqlBuilder innerQueryBuilder, ISqlStatement innerQueryStatement, string alias)
        {
            InnerQuery = (innerQueryBuilder, innerQueryStatement);
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

        private readonly List<(string column, OrderDirection direction)> Ordering = new List<(string, OrderDirection)>();

        public void AddOrderBy(Expression orderBy, OrderDirection direction)
        {
            var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(orderBy, allowOne: true, allowSelect: true);
            if (!isPropertyChain)
                throw new InvalidOperationException($"Invalid order by statement: {orderBy}");

            var ch = chain.ToArray();
            if (ch.Length != 2)
                throw new InvalidOperationException($"Invalid order by statement: {orderBy}");

            Ordering.Add((ch.JoinString("."), direction));
        }

        /// <summary>
        /// A map from a row id column to a location in a mapped property graph
        /// </summary>
        public readonly List<(string rowIdColumnName, string resultClassProperty)> RowIdsForMappedProperties = new List<(string, string)>();

        /// <summary>
        /// A list of joins including their name, sql and any sql which must be run before the query to facilitate the join
        /// </summary>
        readonly List<(string alias, string sql, string setupSql, IEnumerable<string> queryObjectReferences)> _Joins = new List<(string, string, string, IEnumerable<string>)>();
        public IEnumerable<(string alias, string sql, string setupSql, IEnumerable<string> queryObjectReferences)> Joins => _Joins.Skip(0);

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

            // if there are no query object references, add a reference to
            // the root (SELECT) object
            // this can happen if join condition is like "... ON x.Val = 1" 
            var queryObjectReferences = condition.queryObjectReferences.Enumerate();
            if (!queryObjectReferences.Any())
            {
                queryObjectReferences = new [] { PrimaryTableAlias };
            }

            var join = BuildJoin(joinType, joinTable, condition.sql, joinTableAlias);

            _Joins.Add((
                joinTableAlias, 
                join.sql, 
                // combine all setup sql statements
                new [] { condition.setupSql, join.setupSql }
                    .RemoveNullOrEmpty()
                    .JoinString(";\n"),
                queryObjectReferences));
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
        readonly List<(Type cellDataType, string columnName, string tableName, string alias, ConstructorInfo[] argConstructors)> _Select = new List<(Type, string, string, string, ConstructorInfo[])>();

        /// <summary>
        /// A list of columns in the SELECT statement
        /// </summary>
        public IEnumerable<(Type cellDataType, string columnName, string tableName, string alias, ConstructorInfo[] argConstructors)> Select => _Select.Skip(0);
        
        private static readonly ConstructorInfo[] EmptyConstructorInfo = new ConstructorInfo[0];

        /// <summary>
        /// Add a column to the SELECT statement
        /// </summary>
        public void AddSelectColumn(Type cellDataType, string columnName, string tableName = null, string alias = null, ConstructorInfo[] argConstructors = null) =>
            _Select.Add((cellDataType, columnName, tableName, alias, argConstructors ?? EmptyConstructorInfo));

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
        /// <returns>querySetupSql: sql which must be executed before the query is run. 
        /// beforeWhereSql: the query sql before the WHERE statement. 
        /// whereSql: the query sql whereSql the WHERE statement. 
        /// afterWhereSql: the query sql after the WHERE statement</returns>
        public (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) ToSqlString()
        {
            if (PrimaryTable != null && InnerQuery != null)
                throw new InvalidOperationException("You can only call one overload of SetPrimaryTable.");
                
            if (PrimaryTable == null && InnerQuery == null)
                throw new InvalidOperationException("You must call SetPrimaryTable before calling ToSqlString.");
                
            if (PrimaryTableAlias == null)
                throw new InvalidOperationException("You must call SetPrimaryTable before calling ToSqlString.");

            // build SELECT columns (cols and row ids)
            var select = GetAllSelectColumns()
                .Select(s => BuildSelectColumn(s.columnName, s.tableName, s.alias))
                .Enumerate();

            // add placeholder in case no SELECT columns were specified
            if (!select.Any())
                select = new [] { "1" };

            return InnerQuery != null ?
                ToSqlStringWithInnerQuery(select) :
                ToSqlStringWithoutInnerQuery(select);
        }

        /// <summary>
        /// Compile the sql statment to a script where the statement has an inner query
        /// </summary>
        (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) ToSqlStringWithInnerQuery(IEnumerable<string> selectColumns)
        {
            if (Where != null)
                throw new InvalidOperationException($"You can not have an inner query and a WHERE clause. You should put the WHERE clause inside the inner query. WHERE: {Where}");

            if (_Joins.Count != 0)
                throw new InvalidOperationException("You can not have an inner query and a JOIN clause. You should put the JOIN clauses inside the inner query");

            if (Ordering.Count > 0)
                throw new InvalidOperationException("You can not have with an ORDER BY clause. You should put the ORDER BY inside the inner query");

            // get the sql from the inner query if possible
            var (querySetupSql, beforeWhereSql, whereSql, afterWhereSql) = InnerQuery.Value.builder.ToSqlString();

            beforeWhereSql = $"SELECT {selectColumns.JoinString(",")}\nFROM ({beforeWhereSql}";
            afterWhereSql = $"{afterWhereSql}) {SqlBuilder.WrapAlias(PrimaryTableAlias)}";

            return (querySetupSql, beforeWhereSql, whereSql, afterWhereSql);
        }

        /// <summary>
        /// Compile the sql statment to a script
        /// </summary>
        /// <returns>querySetupSql: sql which must be executed before the query is run. querySql: the query sql</returns>
        (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) ToSqlStringWithoutInnerQuery(IEnumerable<string> selectColumns)
        {
            // build WHERE part
            var where = Where == null ? "" : $" WHERE {Where.Value.sql}";

            // build FROM part
            var primaryTable = SqlBuilder.GetSelectTableSqlWithRowId(PrimaryTable, SqlStatementConstants.RowIdName);
                
            // build the order by part
            var orderBy = Ordering
                // TODO: test in indivisual sql languages
                .Select(o => SqlBuilder.WrapAlias(o.column) + (o.direction == OrderDirection.Ascending ? "" : " DESC"))
                .JoinString(", ");

            if (orderBy.Length > 0)
                orderBy = "ORDER BY " + orderBy;
                
            // concat all setup sql from all other parts
            var setupSql = _Joins
                .Select(j => j.setupSql)
                .Concat(new [] 
                {
                    Where?.setupSql,
                    primaryTable.setupSql
                })
                .RemoveNulls()
                .JoinString("\n");

            var query = new[]
            {
                $"\nSELECT {selectColumns.JoinString(",")}",
                $"FROM ({primaryTable.sql}) " + SqlBuilder.WrapAlias(PrimaryTableAlias),
                $"{_Joins.Select(j => j.sql).JoinString("\n")}",
                orderBy
            }
            .Where(x => !string.IsNullOrEmpty(x))
            .JoinString("\n");

            return (setupSql, query, where, "");
        }

        /// <summary>
        /// Build the string for a SELECT column
        /// </summary>
        string BuildSelectColumn(string columnName, string tableName = null, string alias = null)
        {
            alias = alias == null || alias.StartsWith($"{SqlStatementConstants.RootObjectAlias}.") ? "" : $" AS {SqlBuilder.WrapAlias(alias)}";
            columnName = (columnName ?? "").StartsWith("@") ? columnName : SqlBuilder.WrapColumn(columnName);

            return tableName == null ? 
                $"{columnName}{alias}" : 
                $"{SqlBuilder.WrapTable(tableName)}.{columnName}{alias}";
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
                foreach (var join in _Joins)
                {
                    yield return (SqlStatementConstants.RowIdName, join.alias, $"{join.alias}.{SqlStatementConstants.RowIdName}");
                }
            }
            else
            {
                // if there is an inner query, all columns will come from it
                foreach (var table in InnerQuery.Value.statement.Tables)
                {
                    // the only row id will be [inner query alias].[##rowid]
                    yield return (
                        InnerQuery.Value.statement.SelectColumns[table.RowNumberColumnIndex].Alias, 
                        InnerQuery.Value.statement.UniqueAlias, 
                        null);
                }
            }
        }

        /// <summary>
        /// Concat DB table columns with row id columns
        /// </summary>
        IEnumerable<(Type dataType, string columnName, string tableName, string alias, ConstructorInfo[] constructors)> GetAllSelectColumns() =>
            GetRowIdSelectColumns().Select(x => ((Type)null, x.rowIdColumnName, x.tableAlias, x.rowIdColumnNameAlias, EmptyConstructorInfo)).Concat(_Select);
    }
}