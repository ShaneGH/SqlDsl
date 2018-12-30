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
    public class SqlStatementBuilder : SqlStatementBuilderBase, SqlStatementParts.ISqlStatementPartValues
    {
        static int _InnerQueryAlias = 0;
        static readonly object InnerQueryLock = new object();
        
        /// <summary>
        /// The WHERE statement, if necessary
        /// </summary>
        (string setupSql, string sql, IEnumerable<string> queryObjectReferences)? Where = null;

        public readonly string UniqueAlias = BuildInnerQueryAlias();

        readonly List<(string sql, IEnumerable<string> queryObjectReferences, OrderDirection direction)> Ordering = new List<(string, IEnumerable<string>, OrderDirection)>();

        /// <summary>
        /// A list of joins including their name, sql and any sql which must be run before the query to facilitate the join
        /// </summary>
        readonly List<(string alias, string sql, string setupSql, IEnumerable<string> queryObjectReferences)> _Joins = new List<(string, string, string, IEnumerable<string>)>();

        /// <summary>
        /// The name of the table in the SELECT clause
        /// </summary>
        readonly string PrimaryTable;

        public SqlStatementBuilder(ISqlSyntax sqlFragmentBuilder, string primaryTable, string primaryTableAlias)
            : base(sqlFragmentBuilder, primaryTableAlias)
        {
            PrimaryTable = primaryTable ?? throw new ArgumentNullException(nameof(primaryTable));
        }
        
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

        public void AddOrderBy(ParameterExpression queryRootParam, ParameterExpression argsParam, Expression orderBy, OrderDirection direction, ParamBuilder parameters)
        {
            var (sql, queryObjectReferences) = BuildCondition(queryRootParam, argsParam, orderBy, parameters, "ORDER BY");
            Ordering.Add((sql, queryObjectReferences, direction));
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
            joinTableAlias = joinTableAlias == null ? "" : $" {SqlSyntax.WrapAlias(joinTableAlias)}";

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

            var sql = SqlSyntax.GetSelectTableSqlWithRowId(joinTable, SqlStatementConstants.RowIdName);
            return (
                sql.setupSql,
                $"{join} JOIN ({sql.sql}){joinTableAlias} ON {equalityStatement}"
            );
        }

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
            var state = new Mapper.BuildMapState(PrimaryTableAlias, parameters, queryRootParam, queryArgsParam, stat, SqlSyntax);

            var (mp, _) = ComplexMapBuilder.BuildMap(state, conditionStatement);
            var map = mp.ToArray();
            if (map.Length != 1)
                throw new InvalidOperationException($"Invalid {description} condition: {conditionStatement}.");

            var mapSql = map[0].FromParams.BuildFromString(state, SqlSyntax);
            var queryObjectReferences = map[0].FromParams
                .GetEnumerable1()
                .Select(param)
                .Select(table)
                .RemoveNulls();

            return (mapSql, queryObjectReferences);

            string param(Element x) => x.Param;

            string table(string tableAndField)
            {
                var parts = tableAndField.Split('.');
                return parts.Length > 1 ? parts.Take(parts.Length - 1).JoinString(".") : null;
            }
        }

        /// <inheritdoc />
        protected override (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) _ToSqlString(IEnumerable<string> selectColumns, IEnumerable<string> selectTables)
        {
            var allTables = selectTables.ToHashSet();

            // build WHERE part
            var where = Where == null ? "" : $" WHERE {Where.Value.sql}";
            if (Where != null)
                allTables.AddRange(Where.Value.queryObjectReferences);

            // build FROM part
            var primaryTable = SqlSyntax.GetSelectTableSqlWithRowId(PrimaryTable, SqlStatementConstants.RowIdName);
                
            var orderByText = Ordering
                // TODO: test in indivisual sql languages
                .Select(o => o.sql + (o.direction == OrderDirection.Ascending ? "" : $" {SqlSyntax.Descending}"))
                .ToArray();

            allTables.AddRange(Ordering.SelectMany(o => o.queryObjectReferences));

            // build the order by part
            var orderBy = orderByText.Length == 0 ?
                "" :
                orderByText.Aggregate(SqlSyntax.BuildCommaCondition);

            if (orderBy.Length > 0)
                orderBy = "ORDER BY " + orderBy;

            // add tables needed to bridge joins
            foreach (var t in allTables.ToArray())
                allTables.AddRange(GetLineage(t, Enumerable.Empty<string>()));

            var joins = _Joins
                .Where(j => allTables.Contains(j.alias))
                .ToArray();
                
            // concat all setup sql from all other parts
            var setupSql = joins
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
                $"FROM ({primaryTable.sql}) " + SqlSyntax.WrapAlias(PrimaryTableAlias),
                $"{joins.Select(j => j.sql).JoinString("\n")}",
                orderBy
            }
            .Where(x => !string.IsNullOrEmpty(x))
            .JoinString("\n");

            return (setupSql, query, where, "");
        }

        // TODO: this function was copy pasted
        IEnumerable<string> GetLineage(string table, IEnumerable<string> complete)
        {
            if (table == PrimaryTableAlias)
                return table.ToEnumerable();

            if (complete.Contains(table))
                return complete;

            var join = _Joins
                .Where(j => j.alias == table)
                .AsNullable()
                .FirstOrDefault();

            // TODO: add where and order by column tables

            if (join == null)
                throw new InvalidOperationException($"Cannot find join {table}.");

            return join.Value.queryObjectReferences
                .SelectMany(x => GetLineage(x, complete.Append(table)))
                .Append(table);
        }

        /// <summary>
        /// Get a list of row id columns, the alias of the table they are identifying, and the alias for the row id column (if any)
        /// </summary>
        protected override IEnumerable<(string rowIdColumnName, string tableAlias, string rowIdColumnNameAlias)> GetRowIdSelectColumns(IEnumerable<string> selectColumnAliases = null, IEnumerable<string> ensureTableRowIds = null)
        {
            // Get row id from the SELECT
            var ptAlias = PrimaryTableAlias == SqlStatementConstants.RootObjectAlias ? 
                SqlStatementConstants.RowIdName : 
                $"{PrimaryTableAlias}.{SqlStatementConstants.RowIdName}";

            yield return (SqlStatementConstants.RowIdName, PrimaryTableAlias, ptAlias);

            var joins = _Joins.Select(j => j.alias);
            if (selectColumnAliases != null)
            {
                // TODO: inefficient to create disposable version of SqlStatement here
                var stat = new SqlStatement(this);
                var sca = selectColumnAliases
                    .Select(a => stat.SelectColumns[a].Table)
                    .SelectMany(GetTableChain)
                    .Select(t => t.Alias)
                    .Concat(ensureTableRowIds.OrEmpty())
                    .ToHashSet();

                joins = joins.Where(sca.Contains);
            }

            // Get row id from each join
            foreach (var join in joins)
            {
                yield return (
                    SqlStatementConstants.RowIdName, 
                    join, 
                    $"{join}.{SqlStatementConstants.RowIdName}");
            }
        }
            
        static IEnumerable<IQueryTable> GetTableChain(IQueryTable table)
        {
            if (table.JoinedFrom == null)
                return table.ToEnumerable();

            return GetTableChain(table.JoinedFrom).Append(table);
        }

        #region ISqlStatementPartValues

        string ISqlStatementPartValues.UniqueAlias => UniqueAlias;

        string ISqlStatementPartValues.PrimaryTableAlias => PrimaryTableAlias;

        IEnumerable<SqlStatementPartJoin> ISqlStatementPartValues.JoinTables => _Joins.Select(BuildJoinTable);

        IEnumerable<SqlStatementPartSelect> ISqlStatementPartValues.SelectColumns => GetAllSelectColumns().Select(BuildSelectCol);

        static readonly Func<(string alias, string sql, string setupSql, IEnumerable<string> queryObjectReferences), SqlStatementPartJoin> BuildJoinTable = join =>
            new SqlStatementPartJoin(join.alias, join.queryObjectReferences);

        static readonly Func<(bool, SelectColumn), SqlStatementPartSelect> BuildSelectCol = select =>
            new SqlStatementPartSelect(select.Item1, select.Item2.CellDataType, select.Item2.Alias, select.Item2.RepresentsColumns, select.Item2.ArgConstructors);

        #endregion
    }
}