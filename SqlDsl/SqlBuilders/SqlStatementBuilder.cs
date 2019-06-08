using SqlDsl.Mapper;
using SqlDsl.Query;
using SqlDsl.SqlBuilders.SqlStatementParts;
using SqlDsl.SqlExpressions;
using SqlDsl.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    /// <summary>
    /// A class to build sql statements
    /// </summary>
    public class SqlStatementBuilder : ISqlString
    {
        static readonly (string, string) Select1 = ("1", CodingConstants.Null.String);

        public readonly ISqlSyntax SqlSyntax;

        /// <summary>
        /// The name of the table in the SELECT clause
        /// </summary>
        readonly string PrimaryTable;
        
        /// <summary>
        /// The alias of the table in the SELECT clause
        /// </summary>
        public readonly string PrimaryTableAlias;

        /// <summary>
        /// A list of columns in the SELECT statement
        /// </summary>
        readonly List<SelectColumn> _Select = new List<SelectColumn>();

        /// <summary>
        /// The WHERE statement, if necessary
        /// </summary>
        (string sql, IEnumerable<string> queryObjectReferences)? Where = null;

        readonly List<(string sql, IEnumerable<string> queryObjectReferences, OrderDirection direction)> Ordering = new List<(string, IEnumerable<string>, OrderDirection)>();

        /// <summary>
        /// A list of joins including their name, sql and any sql which must be run before the query to facilitate the join
        /// </summary>
        readonly List<(string alias, JoinType joinType, SelectTableSqlWithRowId table, IEnumerable<string> queryObjectReferences)> _Joins = new List<(string, JoinType, SelectTableSqlWithRowId, IEnumerable<string>)>();

        /// <summary>
        /// A list of joins including their name, sql and any sql which must be run before the query to facilitate the join
        /// </summary>
        public IEnumerable<(string alias, JoinType joinType, SelectTableSqlWithRowId table, IEnumerable<string> queryObjectReferences)> Joins => _Joins.Skip(0);

        /// <summary>
        /// A list of columns in the SELECT statement
        /// </summary>
        public IEnumerable<(bool isRowId, SelectColumn col)> AllSelectColumns => GetAllSelectColumns();

        private readonly ReadOnlyCollection<string> PrimaryTableColumns;
        
        /// <summary>If set to true, every join added to the SqlDsl query will also be added to the Sql query.
        /// If false, joins which are not used in a mapping, WHERE clause, ON clause etc... will be automatically removed</summary>
        private bool StrictJoins;

        public SqlStatementBuilder(ISqlSyntax sqlFragmentBuilder, string primaryTable, string primaryTableAlias, IEnumerable<string> primaryTableColumnNames, bool strictJoins)
        {
            SqlSyntax = sqlFragmentBuilder ?? throw new ArgumentNullException(nameof(sqlFragmentBuilder));
            PrimaryTable = primaryTable ?? throw new ArgumentNullException(nameof(primaryTable));
            PrimaryTableAlias = primaryTableAlias ?? throw new ArgumentNullException(nameof(primaryTableAlias));
            PrimaryTableColumns = primaryTableColumnNames?.ToList().AsReadOnly() ?? throw new ArgumentNullException(nameof(primaryTableColumnNames));
            StrictJoins = strictJoins;
        }

        /// <summary>
        /// Add a column to the SELECT statement
        /// </summary>
        public void AddSelectColumn(Type cellDataType, string table, string column, string alias, ConstructorInfo[] argConstructors = null) =>
            _Select.Add(new SelectColumn(cellDataType, table, column, alias ?? throw new ArgumentNullException(nameof(alias)), argConstructors ?? CodingConstants.Empty.ConstructorInfo));

        public void AddOrderBy(ParameterExpression queryRootParam, ParameterExpression argsParam, Expression orderBy, OrderDirection direction, ParamBuilder parameters)
        {
            var (sql, queryObjectReferences) = BuildCondition(queryRootParam, argsParam, orderBy, parameters, MappingPurpose.OrderBy);
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
            IEnumerable<string> joinTableColumnNames, 
            ParameterExpression queryRootParam, 
            ParameterExpression queryArgsParam,
            ParameterExpression joinTableParam,
            Expression equalityStatement, 
            ParamBuilder parameters, 
            string joinTableAlias)
        {
            // convert (q, j) => q.Table1.Id == j.Table1Id - or -
            // convert (q, j) => q.Table1.Id == j.One().Table1Id
            // to
            // q => q.Table1.Id == q.Table2.One().Table1Id
            var addOne = ReflectionUtils.GetIEnumerableType(joinTableParam.Type) == null;
            var joinTableProp = joinTableAlias
                .Split('.')
                .Aggregate(
                    queryRootParam as Expression,
                    AddJoinProperty);
            
            equalityStatement = ParameterReplacer.ReplaceParameter(equalityStatement, joinTableParam, joinTableProp);
            var (sql, queryObjectReferences) = BuildCondition(queryRootParam, queryArgsParam, equalityStatement, parameters, MappingPurpose.JoinOn);
            queryObjectReferences = queryObjectReferences.Where(x => x != joinTableAlias);

            // if there are no query object references, add a reference to
            // the root (SELECT) object
            // this can happen if join condition is like "... ON x.Val = 1" 
            queryObjectReferences = queryObjectReferences.Enumerate();
            if (!queryObjectReferences.Any())
            {
                queryObjectReferences = new [] { PrimaryTableAlias };
            }

            var join = BuildJoin(joinType, joinTable, joinTableColumnNames, sql, joinTableAlias);

            _Joins.Add((
                joinTableAlias, 
                joinType,
                join,
                queryObjectReferences.Where(r => r != joinTableAlias)));

            Expression AddJoinProperty(Expression rootExpression, string property)
            {
                Expression rawProp = Expression.PropertyOrField(rootExpression, property);
                if (addOne)
                {
                    var enumerableType = ReflectionUtils.GetIEnumerableType(rawProp.Type);
                    if (enumerableType != null)
                    {
                        rawProp = Expression.Call(
                            ReflectionUtils.GetMethod(() => new string[0].One(), enumerableType),
                            rawProp);
                    }
                }

                return rawProp;
            }
        }

        /// <summary>
        /// Build JOIN sql
        /// </summary>
        SelectTableSqlWithRowId BuildJoin(JoinType joinType, string joinTable, IEnumerable<string> allColumns, string equalityStatement, string joinTableAlias = null)
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

            var sqlTable = SqlSyntax.GetSelectTableSqlWithRowId(joinTable, SqlStatementConstants.RowIdName, allColumns);
            return new SelectTableSqlWithRowId(
                sqlTable.SetupSql,
                $"{join} JOIN ({sqlTable.Sql}){joinTableAlias} ON {equalityStatement}",
                sqlTable.TeardownSql,
                sqlTable.TeardownSqlCanBeInlined
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
            var (whereSql, queryObjectReferences) = BuildCondition(queryRoot, args, equality, parameters, MappingPurpose.Where);
            Where = (whereSql, queryObjectReferences);
        }

        (string sql, IEnumerable<string> queryObjectReferences) BuildCondition(
            ParameterExpression queryRootParam, 
            ParameterExpression queryArgsParam,
            Expression conditionStatement, 
            ParamBuilder parameters,
            MappingPurpose mapping)
        {
            var stat = new SqlStatementParts.SqlStatement(this);
            var state = new Mapper.BuildMapState(PrimaryTableAlias, parameters, queryRootParam, queryArgsParam, stat, SqlSyntax, false, mapping);

            var (mp, _) = ComplexMapBuilder.BuildMap(state, conditionStatement);
            var map = mp.ToArray();
            if (map.Length != 1)
                throw new InvalidOperationException($"Invalid {mapping} condition: {conditionStatement}.");

            var mapSql = map[0].FromParams.BuildFromString(state, SqlSyntax, null);
            var queryObjectReferences = map[0].FromParams
                .GetEnumerable()
                .Select(param)
                .Select(table)
                .RemoveNulls();

            return (mapSql, queryObjectReferences);

            string param(StringBasedElement x) => x.Param;

            string table(string tableAndField)
            {
                var parts = tableAndField.Split('.');
                return parts.Length > 1 ? parts.Take(parts.Length - 1).JoinString(".") : null;
            }
        }

        /// <inheritdoc />
        public SqlString ToSqlString(IEnumerable<string> selectColumnAliases = null)
        {
            var rowIds = GetRowIdSelectColumns(selectColumnAliases).Enumerate();
            if (!rowIds.Any())
            {
                // there must be at least 1 row id
                rowIds = GetRowIdSelectColumns()
                    .Take(1)
                    .Enumerate();
            }

            IEnumerable<SelectColumn> selects = _Select;
            if (selectColumnAliases != null)
            {
                var sca = selectColumnAliases.ToHashSet();
                selects = selects.Where(c => sca.Contains(c.Alias));
            }

            // TODO: look at $"{rid.tableAlias}.{rid.rowIdColumnName}". What if tableAlias is null or #root
            var sels = rowIds
                .Select(rid => (sql: BuildSqlForRid(rid), table: rid.tableAlias))
                .Concat(selects.Select(sel => (sql: SqlSyntax.AddAliasColumn(SqlSyntax.BuildSelectColumn(sel.Table, sel.Column), sel.Alias), table: sel.Table)))
                .ToList();

            // if there is absolutely nothing to select, prevent error by selecting 1
            if (sels.Count == 0)
                sels.Add(Select1);

            return ToSqlString(
                sels.Select(c => c.sql), 
                sels
                    .Select(c => c.table)
                    .RemoveNulls()
                    .Distinct());

            string BuildSqlForRid((string rowIdColumnName, string tableAlias, string rowIdColumnNameAlias) rid)
            {
                var select = SqlSyntax.BuildSelectColumn(rid.tableAlias, rid.rowIdColumnName);
                var alias = rid.tableAlias == null
                    ? rid.rowIdColumnName
                    : rid.rowIdColumnName == null
                        ? rid.tableAlias
                        : $"{rid.tableAlias}.{rid.rowIdColumnName}";

                return SqlSyntax.AddAliasColumn(select, alias);
            }
        }    

        /// <inheritdoc />
        SqlString ToSqlString(IEnumerable<string> selectColumns, IEnumerable<string> selectTables)
        {
            var allTables = selectTables.ToHashSet();

            // build WHERE part
            var where = Where == null ? "" : $"WHERE {Where.Value.sql}";
            if (Where != null)
                allTables.AddRange(Where.Value.queryObjectReferences);

            // build FROM part
            var primaryTable = SqlSyntax.GetSelectTableSqlWithRowId(PrimaryTable, SqlStatementConstants.RowIdName, PrimaryTableColumns);
                
            var orderByText = Ordering
                // TODO: test in individual sql languages
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

            var joins = StrictJoins
                ? _Joins
                : _Joins
                    .Where(j => allTables.Contains(j.alias))
                    .ToList();
                
            // concat all setup sql from all other parts
            var setupSql = joins
                .Select(j => j.table.SetupSql)
                .Append(primaryTable.SetupSql)
                .RemoveNulls()
                .JoinString("\n");
                
            // concat all teardown sql from all other parts
            var teardownSql = joins
                .Select(j => j.table.TeardownSql)
                .Append(primaryTable.TeardownSql)
                .RemoveNulls()
                .JoinString("\n");

            var newQueryForTeardown = joins
                .Select(j => j.table.TeardownSqlCanBeInlined)
                .Append(primaryTable.TeardownSqlCanBeInlined)
                .Any(x => !x);

            var query = new[]
            {
                $"\nSELECT {selectColumns.JoinString(",")}",
                $"FROM ({primaryTable.Sql}) " + SqlSyntax.WrapAlias(PrimaryTableAlias),
                $"{joins.Select(j => j.table.Sql).JoinString("\n")}"
            }
            .Where(x => !string.IsNullOrEmpty(x))
            .JoinString("\n");

            return new SqlString(setupSql, query, $"\n{where}", $"\n{orderBy}", teardownSql, !newQueryForTeardown);
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
                throw new InvalidOperationException($"Cannot find join for {table}.");

            return join.Value.queryObjectReferences
                .SelectMany(x => GetLineage(x, complete.Append(table)))
                .Append(table);
        }

        /// <summary>
        /// Concat DB table columns with row id columns
        /// </summary>
        protected IEnumerable<(bool isRowId, SelectColumn col)> GetAllSelectColumns() =>
            GetRowIdSelectColumns()
            .Select(x => (true, new SelectColumn((Type)null, x.tableAlias, x.rowIdColumnName, x.rowIdColumnNameAlias, CodingConstants.Empty.ConstructorInfo)))
            .Concat(_Select.Select(x => (false, x)));

        /// <summary>
        /// Get a list of row id columns, the alias of the table they are identifying, and the alias for the row id column (if any)
        /// </summary>
        IEnumerable<(string rowIdColumnName, string tableAlias, string rowIdColumnNameAlias)> GetRowIdSelectColumns(IEnumerable<string> selectColumnAliases = null)
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
                    .Select(a => stat.SelectColumns[a].RowNumberColumn.IsRowNumberForTable)
                    .SelectMany(ISqlSelectStatementUtils.GetAllReferencedTables)
                    .Select(t => t.Alias)
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

        public class SelectColumn
        {
            public readonly Type CellDataType;
            public readonly string Table;
            public readonly string Column;
            public readonly  string Alias;
            public readonly  ConstructorInfo[] ArgConstructors;

            public SelectColumn(
                Type cellDataType,
                string table, 
                string column,
                string alias, 
                ConstructorInfo[] argConstructors)
            {
                CellDataType = cellDataType;
                Table = table;
                Column = column;
                Alias = alias;
                ArgConstructors = argConstructors;
            }
        }
    }
}