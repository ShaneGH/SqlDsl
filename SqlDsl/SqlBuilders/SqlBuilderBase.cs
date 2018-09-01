using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    using OtherParams = IEnumerable<(ParameterExpression param, string alias)>;

    /// <summary>
    /// Base class for building sql statements. Inherit from this class to build different flavours of sql
    /// </summary>
    public abstract class SqlBuilderBase : ISqlBuilder
    {
        public static readonly string RowIdName = "##rowid";
        public static readonly string RootObjectAlias = "##root";
        
        //TODO: not thread safe
        static int _InnerQueryAlias = 0;
        public string InnerQueryAlias { get; private set; } = $"iq{++_InnerQueryAlias}";

        IEnumerable<(string alias, string sql)> ISqlBuilder.Joins => Joins.Skip(0);

        readonly List<(string alias, string sql)> Joins = new List<(string alias, string sql)>();
        public void AddJoin(
            JoinType joinType, 
            string joinTable, 
            ParameterExpression queryRootParam, 
            ParameterExpression joinTableParam,
            Expression equalityStatement, 
            IList<object> paramaters, 
            string joinTableAlias)
        {
            Joins.Add((joinTableAlias,
                BuildJoin(
                    joinType, 
                    joinTable, 
                    BuildCondition(queryRootParam, new[]{(joinTableParam, joinTableAlias)}, equalityStatement, paramaters),
                    joinTableAlias)));
        }

        /// <summary>
        /// Build a sql statement which selects * from a table and adds a unique row id named {rowIdAlias}
        /// </summary>
        protected abstract string GetUniqueIdSql(string tableName, string rowIdAlias);

        protected virtual string BuildJoin(JoinType joinType, string joinTable, string equalityStatement, string joinTableAlias = null)
        {
            joinTableAlias = joinTableAlias == null ? "" : $" {WrapAlias(joinTableAlias)}";

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

            return $"{join} JOIN ({GetUniqueIdSql(joinTable, RowIdName)}){joinTableAlias} ON {equalityStatement}";
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
            alias = alias == null || alias.StartsWith($"{RootObjectAlias}.") ? "" : $" AS {WrapAlias(alias)}";

            return tableName == null ? 
                $"{WrapColumn(columnName)}{alias}" : 
                $"{WrapTable(tableName)}.{WrapColumn(columnName)}{alias}";
        }

        IEnumerable<(string columnName, string tableName, string alias)> GetRowIdSelectColumns()
        {
            if (InnerQuery == null)
            {
                foreach (var join in Joins)
                {
                    yield return (RowIdName, join.alias, $"{join.alias}.{RowIdName}");
                }

                var ptAlias = PrimaryTableAlias == RootObjectAlias ? null : $"{PrimaryTableAlias}.{RowIdName}";
                yield return (RowIdName, PrimaryTableAlias, ptAlias);
            }
            else
            {
                foreach (var rowId in InnerQuery.RowIdSelectColumns)
                {
                    yield return (rowId.alias ?? rowId.columnName, InnerQuery.InnerQueryAlias, null);
                }
            }
            
            // var rowNumbers = InnerQuery == null ?
            //     Joins
            //         .Concat(new[] { (alias: PrimaryTableAlias, "") })
            //         .Select(j => BuildSelectColumn(RowIdName, j.alias, $"{j.alias}.{RowIdName}") + ",")
            //         .JoinString("") :
            //     InnerQuery.Joins
            //         .Concat(new[] { (alias: InnerQuery.PrimaryTableAlias, "") })
            //         .Select(j => BuildSelectColumn($"{Alias(j.alias)}{RowIdName}", tableName: InnerQuery.InnerQueryAlias) + ",")
            //         .JoinString("");
            // string Alias(string actualAlias) =>
            //     actualAlias == RootObjectAlias ? "" : (actualAlias + ".");
        }

        IEnumerable<(string columnName, string tableName, string alias)> GetAllSelectColumns() =>
            GetRowIdSelectColumns().Concat(Select);

        string Where = null;
        public void SetWhere(ParameterExpression queryRoot, Expression equality, IList<object> paramaters)
        {
            Where = BuildCondition(queryRoot, Enumerable.Empty<(ParameterExpression, string)>(), equality, paramaters);
        }

        readonly List<string> BeforeStatements = new List<string>();
        protected void AddStatementBefore(string statement)
        {
            BeforeStatements.Add(statement);
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

            var select = AllSelectColumns
                .Select(s => BuildSelectColumn(s.columnName, s.tableName, s.alias));

            var innerQuery = InnerQuery?.ToSqlString();

            var where = Where == null ? "" : $"WHERE {Where}";

            var primaryTable = innerQuery != null ?
                innerQuery.Value.querySql :
                GetUniqueIdSql(PrimaryTable, RowIdName);

            var beforeStatement = (innerQuery?.querySetupSql)
                .ToEnumerable()
                .Concat(new[] { BeforeStatements.JoinString("\n") })
                .JoinString("\n");

            var query = new[]
            {
                $"SELECT {select.JoinString(",")}",
                $"FROM ({primaryTable}) " + WrapAlias(PrimaryTableAlias),
                $"{Joins.Select(j => j.sql).JoinString("\n")}",
                $"{where}"
            }
            .Where(x => !string.IsNullOrEmpty(x))
            .JoinString("\n");

            return (beforeStatement, query);
        }

        public virtual string TableName(string name, string schema = null)
        {
            schema = schema == null ? "" : $"{WrapTable(schema)}.";
            return schema + $"{WrapTable(name)}";
        }

        protected virtual string BuildCondition(ParameterExpression queryRoot, OtherParams otherParams, Expression equality, IList<object> paramaters)
        {
            switch (equality.NodeType)
            {
                case ExpressionType.Convert:
                    return BuildCondition(queryRoot, otherParams, (equality as UnaryExpression).Operand, paramaters);
                case ExpressionType.AndAlso:
                    return BuildAndCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.OrElse:
                    return BuildOrCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.Equal:
                    return BuildEqualityCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.NotEqual:
                    return BuildNonEqualityCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.LessThan:
                    return BuildLessThanCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.LessThanOrEqual:
                    return BuildLessThanEqualToCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.GreaterThan:
                    return BuildGreaterThanCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.GreaterThanOrEqual:
                    return BuildGreaterThanEqualToCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.MemberAccess:
                    return BuildMemberAccessCondition(queryRoot, otherParams, equality as MemberExpression, paramaters);
                case ExpressionType.Constant:
                    return BuildConstantCondition(equality as ConstantExpression, paramaters);
                default:
                    throw new NotImplementedException($"Cannot compile expression: {equality.NodeType} to SQL");
            }
        }

        protected virtual string BuildAndCondition(ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression and, IList<object> paramaters) =>
            $"({BuildCondition(queryRoot, otherParams, and.Left, paramaters)} AND {BuildCondition(queryRoot, otherParams, and.Right, paramaters)})";

        protected virtual string BuildOrCondition(ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression or, IList<object> paramaters) =>
            $"({BuildCondition(queryRoot, otherParams, or.Left, paramaters)} OR {BuildCondition(queryRoot, otherParams, or.Right, paramaters)})";

        protected virtual string BuildEqualityCondition(ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression eq, IList<object> paramaters) =>
            $"({BuildCondition(queryRoot, otherParams, eq.Left, paramaters)} = {BuildCondition(queryRoot, otherParams, eq.Right, paramaters)})";

        protected virtual string BuildNonEqualityCondition(ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression eq, IList<object> paramaters) =>
            $"({BuildCondition(queryRoot, otherParams, eq.Left, paramaters)} <> {BuildCondition(queryRoot, otherParams, eq.Right, paramaters)})";

        protected virtual string BuildLessThanCondition(ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression eq, IList<object> paramaters) =>
            $"({BuildCondition(queryRoot, otherParams, eq.Left, paramaters)} < {BuildCondition(queryRoot, otherParams, eq.Right, paramaters)})";

        protected virtual string BuildLessThanEqualToCondition(ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression eq, IList<object> paramaters) =>
            $"({BuildCondition(queryRoot, otherParams, eq.Left, paramaters)} <= {BuildCondition(queryRoot, otherParams, eq.Right, paramaters)})";

        protected virtual string BuildGreaterThanCondition(ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression eq, IList<object> paramaters) =>
            $"({BuildCondition(queryRoot, otherParams, eq.Left, paramaters)} > {BuildCondition(queryRoot, otherParams, eq.Right, paramaters)})";

        protected virtual string BuildGreaterThanEqualToCondition(ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression eq, IList<object> paramaters) =>
            $"({BuildCondition(queryRoot, otherParams, eq.Left, paramaters)} >= {BuildCondition(queryRoot, otherParams, eq.Right, paramaters)})";

        (bool memberHasStaticValue, object memberStaticValue) GetMemberStaticObjectValue(MemberExpression member)
        {
            var m = member;
            while (member != null)
            {
                // if static property, expression == null
                if (member.Expression is ConstantExpression || member.Expression == null)
                {
                    var t = DateTime.Now;
                    var valueGetter = Expression
                        .Lambda<Func<object>>(
                            Expression.Convert(
                                m, typeof(object)))
                        .Compile();

                    return (true, valueGetter());
                }

                member = member.Expression as MemberExpression;
            }

            return (false, null);
        }

        (bool memberIsFromQueryObject, IEnumerable<string> memberQueryObjectParts, ParameterExpression rootParam) GetMemberQueryObjectName(MemberExpression member)
        {
            if (member.Expression is ParameterExpression)
                return (true, new [] { member.Member.Name }, member.Expression as ParameterExpression);

            if (member.Expression is MemberExpression)
            {
                var inner = GetMemberQueryObjectName(member.Expression as MemberExpression);
                return inner.memberIsFromQueryObject ?
                    (true, inner.memberQueryObjectParts.Concat(new[]{member.Member.Name}), inner.rootParam) :
                    (false, null, null);
            }

            var oneOf = ReflectionUtils.IsOne(member.Expression) as MemberExpression;
            if (oneOf != null)
            {
                var inner = GetMemberQueryObjectName(oneOf);
                return inner.memberIsFromQueryObject ?
                    (true, inner.memberQueryObjectParts.Concat(new[]{member.Member.Name}), inner.rootParam) :
                    (false, null, null);
            }

            return (false, null, null);
        }

        protected virtual string BuildMemberAccessCondition(
            ParameterExpression queryRoot, 
            OtherParams otherParams, 
            MemberExpression member, 
            IList<object> paramaters)
        {
            // e.g. if overall join expression is TQuery.Entity1.Id == Entity2.Id
            // then, "propertyExpression" input will be lhs -or- rhs of above

            var staticValue = GetMemberStaticObjectValue(member);
            if (staticValue.memberHasStaticValue)
                return AddToParamaters(staticValue.memberStaticValue, paramaters);

            // propertyExpression is "TQuery.Entity1.Id" part
            var tableValue = GetMemberQueryObjectName(member);
            if (tableValue.memberIsFromQueryObject)
            {
                var parts = tableValue.memberQueryObjectParts.ToList();
                var paramAlias = otherParams
                    .Where(op => op.param == tableValue.rootParam)
                    .Select(op => op.alias)
                    .FirstOrDefault();

                if (paramAlias != null)
                    parts.Insert(0, paramAlias);

                var table = parts.Count > 1 ? 
                    WrapAlias(parts
                        .Take(parts.Count - 1)
                        .JoinString(".")) + "." :
                    "";

                return $"{table}{WrapColumn(parts.Last())}";
            }

            throw new InvalidOperationException($"Cannot find table for expression ${member}");
        }

        protected virtual string BuildConstantCondition(ConstantExpression constant, IList<object> paramaters) => 
            AddToParamaters(constant.Value, paramaters);

        string AddToParamaters(object value, IList<object> paramaters)
        {
            paramaters.Add(value);
            return $"@p{paramaters.Count - 1}";
        }

        /// <summary>
        /// Wrap a table name in parenthesis whcih protect against illegal characters
        /// </summary>
        protected abstract string WrapTable(string table);

        /// <summary>
        /// Wrap a column name in parenthesis whcih protect against illegal characters
        /// </summary>
        protected abstract string WrapColumn(string column);

        /// <summary>
        /// Wrap a table or column alias in parenthesis whcih protect against illegal characters
        /// </summary>
        protected abstract string WrapAlias(string alias);

        string PrimaryTable;
        public string PrimaryTableAlias { get; private set; }
        public void SetPrimaryTable(string tableName, string alias)
        {
            PrimaryTable = tableName;
            PrimaryTableAlias = alias;
        }

        ISqlBuilder InnerQuery;

        public void SetPrimaryTable(ISqlBuilder table, string alias)
        {
            InnerQuery = table;
            PrimaryTableAlias = alias;
        }
    }
}
