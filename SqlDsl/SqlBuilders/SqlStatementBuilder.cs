using SqlDsl.Mapper;
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
        readonly IEnumerable<string> EmptyStrings = new string[0];

        public readonly ISqlFragmentBuilder SqlBuilder;

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

        private readonly List<(string sql, OrderDirection direction)> Ordering = new List<(string, OrderDirection)>();

        public void AddOrderBy(ParameterExpression queryRootParam, ParameterExpression argsParam, Expression orderBy, OrderDirection direction, ParamBuilder parameters)
        {
<<<<<<< HEAD
            var (sql, _) = BuildCondition(queryRootParam, argsParam, orderBy, parameters, "ORDER BY");
            Ordering.Add((sql, direction));
=======
            var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(orderBy, allowOne: true, allowSelect: true);
            if (!isPropertyChain)
                throw new InvalidOperationException($"Invalid order by statement: {orderBy}");

            Ordering.Add((chain.JoinString("."), direction));
>>>>>>> todos
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

        class ParameterReplacer : ExpressionVisitor, IDisposable
        {
            ParameterExpression Parameter;
            Expression Replacement;

            private ParameterReplacer()
            {
            }

            void Init(ParameterExpression parameter, Expression replacement)
            {
                Parameter = parameter;
                Replacement = replacement;
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                if (node != Parameter)
                    return base.VisitParameter(node);

                return Replacement;
            }

            public void Dispose() => Init(null, null);

            [ThreadStatic]
            static readonly ParameterReplacer Instance = new ParameterReplacer();

            public static Expression ReplaceParameter(Expression expr, ParameterExpression parameter, Expression replacement)
            {
                using (Instance)
                {
                    Instance.Init(parameter, replacement);
                    return Instance.Visit(expr);
                }
            }
        }

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
            ParamBuilder parameters, 
            string joinTableAlias)
        {
            // convert (q, j) => q.Table1.Id == j.Table1Id
            // to
            // q => q.Table1.Id == q.Table2.One().Table1Id
            var joinTableProp = joinTableAlias
                .Split('.')
                .Aggregate(
                    queryRootParam as Expression,
                    AddJoinProperty);
            
            equalityStatement = ParameterReplacer.ReplaceParameter(equalityStatement, joinTableParam, joinTableProp);
            var (sql, queryObjectReferences) = BuildCondition(queryRootParam, queryArgsParam, equalityStatement, parameters, "JOIN ON");
            queryObjectReferences = queryObjectReferences.Where(x => x != joinTableAlias);

            // if there are no query object references, add a reference to
            // the root (SELECT) object
            // this can happen if join condition is like "... ON x.Val = 1" 
            queryObjectReferences = queryObjectReferences.Enumerate();
            if (!queryObjectReferences.Any())
            {
                queryObjectReferences = new [] { PrimaryTableAlias };
            }

            var join = BuildJoin(joinType, joinTable, sql, joinTableAlias);

            _Joins.Add((
                joinTableAlias, 
                join.sql, 
                // combine all setup sql statements
                join.setupSql,
                queryObjectReferences.Where(r => r != joinTableAlias)));

            Expression AddJoinProperty(Expression rootExpression, string property)
            {
                Expression rawProp = Expression.PropertyOrField(rootExpression, property);
                var enumerableType = ReflectionUtils.GetIEnumerableType(rawProp.Type);
                if (enumerableType != null)
                {
                    rawProp = Expression.Call(
                        ReflectionUtils.GetMethod(() => new string[0].One(), enumerableType),
                        rawProp);
                }

                return rawProp;
            }
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
                // TODO: put in fragment builder
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
        readonly List<(Type cellDataType, string selectCode, string alias, (string table, string column, bool isAggregate)[] representsColumns, ConstructorInfo[] argConstructors)> _Select = new List<(Type, string, string, (string, string, bool)[], ConstructorInfo[])>();

        /// <summary>
        /// A list of columns in the SELECT statement
        /// </summary>
        public IEnumerable<(Type cellDataType, string selectCode, string alias, (string table, string column, bool isAggregate)[] representsColumns, ConstructorInfo[] argConstructors)> Select => _Select.Skip(0);
        
        private static readonly ConstructorInfo[] EmptyConstructorInfo = new ConstructorInfo[0];

        /// <summary>
        /// Add a column to the SELECT statement
        /// </summary>
        public void AddSelectColumn(Type cellDataType, string selectCode, string alias, (string table, string column, bool isAggregate)[] representsColumns, ConstructorInfo[] argConstructors = null) =>
            _Select.Add((cellDataType, selectCode, alias ?? throw new ArgumentNullException(nameof(alias)), representsColumns, argConstructors ?? EmptyConstructorInfo));

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
        public void SetWhere(ParameterExpression queryRoot, ParameterExpression args, Expression equality, ParamBuilder parameters)
        {
            var (whereSql, queryObjectReferences) = BuildCondition(queryRoot, args, equality, parameters, "WHERE");
            Where = ("", whereSql, queryObjectReferences);
        }

        (string sql, IEnumerable<string> queryObjectReferences) BuildCondition(
            ParameterExpression queryRootParam, 
            ParameterExpression queryArgsParam,
            Expression conditionStatement, 
            ParamBuilder parameters,
            string description)
        {
            var stat = new SqlStatementParts.SqlStatement(this);
            var state = new Mapper.BuildMapState(PrimaryTableAlias, parameters, queryRootParam, queryArgsParam, stat);

            var (mp, _) = ComplexMapBuilder.BuildMap(state, conditionStatement);
            var map = mp.ToArray();
            if (map.Length != 1)
                throw new InvalidOperationException($"Invalid {description} condition: {conditionStatement}.");

            var mapSql = map[0].FromParams.BuildFromString(state, SqlBuilder);
            var queryObjectReferences = map[0].FromParams
                .GetEnumerable1()
                .Select(param)
                .Select(table)
                .RemoveNulls();

            return (mapSql, queryObjectReferences);

            string param((ParameterExpression, string) x) => x.Item2;

            string table(string tableAndField)
            {
                var parts = tableAndField.Split('.');
                return parts.Length > 1 ? parts.Take(parts.Length - 1).JoinString(".") : null;
            }
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
                .Select(s => SqlBuilder.AddAliasColumn(s.selectCode, s.alias))
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
            afterWhereSql = $"{afterWhereSql}) {SqlBuilder.WrapAlias(PrimaryTableAlias)}{BuildGroupByStatement("\n")}";

            return (querySetupSql, beforeWhereSql, whereSql, afterWhereSql);
        }

        string BuildGroupByStatement(string prefix)
        {
            if (GetAllSelectColumns().All(cs => cs.representsColumns.All(c => !c.isAggregate)))
                return "";

            var output = new List<string>(16);
            foreach (var col in GetAllSelectColumns()
                .SelectMany(cs => cs.representsColumns)
                .Where(c => !c.isAggregate))
            {
                output.Add(SqlBuilder.BuildSelectColumn(col.table, col.column));
            }

            if (output.Count > 0)
                prefix += "GROUP BY ";

            return $"{prefix}{output.JoinString(",")}";
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
                
            var orderByText = Ordering
                // TODO: test in indivisual sql languages
                .Select(o => o.sql + (o.direction == OrderDirection.Ascending ? "" : $" {SqlBuilder.Descending}"))
                .ToArray();

            // build the order by part
            var orderBy = orderByText.Length == 0 ?
                "" :
                orderByText.Aggregate(SqlBuilder.BuildCommaCondition);

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
        IEnumerable<(Type dataType, string selectCode, string alias, (string table, string column, bool isAggregate)[] representsColumns, ConstructorInfo[] constructors)> GetAllSelectColumns() =>
            GetRowIdSelectColumns().Select(x => ((Type)null, SqlBuilder.BuildSelectColumn(x.tableAlias, x.rowIdColumnName), x.rowIdColumnNameAlias, new [] { (x.tableAlias, x.rowIdColumnName, false) }, EmptyConstructorInfo)).Concat(_Select);

        /// <summary>
        /// Remove any tables from the query which are not in the requiredTableAliases list
        /// </summary>
        public void FilterUnusedTables(IEnumerable<string> requiredTableAliases)
        {
            var tables = new HashSet<string>(requiredTableAliases
                .SelectMany(t => GetLineage(t, EmptyStrings)));

            for (var i = _Joins.Count - 1; i >= 0; i--)
            {
                if (!tables.Contains(_Joins[i].alias))
                    _Joins.RemoveAt(i);
            }

            for (var i = _Select.Count - 1; i >= 0; i--)
            {
                if (_Select[i].representsColumns.Any(c => !tables.Contains(c.table)))
                    _Select.RemoveAt(i);
            }
        }

        IEnumerable<string> GetLineage(string table, IEnumerable<string> complete)
        {
            if (table == PrimaryTableAlias)
                return table.ToEnumerable();

            if (complete.Contains(table))
                return complete;

            var join = Joins
                .Where(j => j.alias == table)
                .AsNullable()
                .FirstOrDefault();

            if (join == null)
                throw new InvalidOperationException($"Cannot find join {table}.");

            return join.Value.queryObjectReferences
                .SelectMany(x => GetLineage(x, complete.Append(table)))
                .Append(table);
        }
    }
}