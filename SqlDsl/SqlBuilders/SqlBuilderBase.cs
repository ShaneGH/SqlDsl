using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.Query;
using SqlDsl.Utils;

namespace SqlDsl.SqlBuilders
{
    /// <summary>
    /// Base class for building sql statements. Inherit from this class to build different flavours of sql
    /// </summary>
    public abstract class SqlBuilderBase : ISqlBuilder
    {
        public static readonly string RowIdName = "##rowid";
        
        static int _InnerQueryAlias = 0;
        public string InnerQueryAlias { get; private set; } = $"iq{++_InnerQueryAlias}";

        IEnumerable<(string alias, string sql)> ISqlBuilder.Joins => Joins.Skip(0);

        readonly List<(string alias, string sql)> Joins = new List<(string alias, string sql)>();
        public void AddJoin(JoinType joinType, string joinTable, (IEnumerable<(string name, MemberInfo token, ParameterExpression reference)> tables, Expression equality, IList<object> paramaters) equalityStatement, string joinTableAlias = null)
        {
            Joins.Add((joinTableAlias,
                BuildJoin(
                    joinType, 
                    joinTable, 
                    BuildCondition(equalityStatement.tables, equalityStatement.equality, equalityStatement.paramaters),
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

        readonly List<string> Select = new List<string>();
        public void AddSelectColumn(string columnName, string tableName = null, string alias = null)
        {
            Select.Add(BuildSelectColumn(columnName, tableName, alias));
        }

        public virtual string BuildSelectColumn(string columnName, string tableName = null, string alias = null)
        {
            alias = alias == null ? "" : $" as {WrapAlias(alias)}";

            return tableName == null ? 
                $"{WrapColumn(columnName)}{alias}" : 
                $"{WrapTable(tableName)}.{WrapColumn(columnName)}{alias}";
        }

        string Where = null;
        public void SetWhere(IEnumerable<(string name, MemberInfo token, ParameterExpression reference)> tables, Expression equality, IList<object> paramaters)
        {
            Where = BuildCondition(tables, equality, paramaters);
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

            var innerQuery = InnerQuery?.ToSqlString();

            var where = Where == null ? "" : $"WHERE {Where}";
            var rowNumbers = InnerQuery == null ?
                Joins
                    .Concat(new[] { (alias: PrimaryTableAlias, "") })
                    .Select(j => BuildSelectColumn(RowIdName, j.alias, $"{j.alias}.{RowIdName}") + ",")
                    .JoinString("") :
                InnerQuery.Joins
                    .Concat(new[] { (alias: InnerQuery.PrimaryTableAlias, "") })
                    .Select(j => BuildSelectColumn($"{j.alias}.{RowIdName}", tableName: InnerQuery.InnerQueryAlias) + ",")
                    .JoinString("");

            var primaryTable = innerQuery != null ?
                innerQuery.Value.querySql :
                GetUniqueIdSql(PrimaryTable, RowIdName);

            var beforeStatement = (innerQuery?.querySetupSql)
                .ToEnumerable()
                .Concat(new[] { BeforeStatements.JoinString("\n") })
                .JoinString("\n");

            var query = new[]
            {
                $"SELECT {rowNumbers}{Select.JoinString(",")}",
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

        protected virtual string BuildCondition(IEnumerable<(string name, MemberInfo token, ParameterExpression reference)> tables, Expression equality, IList<object> paramaters)
        {
            switch (equality.NodeType)
            {
                case ExpressionType.Convert:
                    return BuildCondition(tables, (equality as UnaryExpression).Operand, paramaters);
                case ExpressionType.AndAlso:
                    return BuildAndCondition(tables, equality as BinaryExpression, paramaters);
                case ExpressionType.OrElse:
                    return BuildOrCondition(tables, equality as BinaryExpression, paramaters);
                case ExpressionType.Equal:
                    return BuildEqualityCondition(tables, equality as BinaryExpression, paramaters);
                case ExpressionType.NotEqual:
                    return BuildNonEqualityCondition(tables, equality as BinaryExpression, paramaters);
                case ExpressionType.LessThan:
                    return BuildLessThanCondition(tables, equality as BinaryExpression, paramaters);
                case ExpressionType.LessThanOrEqual:
                    return BuildLessThanEqualToCondition(tables, equality as BinaryExpression, paramaters);
                case ExpressionType.GreaterThan:
                    return BuildGreaterThanCondition(tables, equality as BinaryExpression, paramaters);
                case ExpressionType.GreaterThanOrEqual:
                    return BuildGreaterThanEqualToCondition(tables, equality as BinaryExpression, paramaters);
                case ExpressionType.MemberAccess:
                    return BuildMemberAccessCondition(tables, equality as MemberExpression, paramaters);
                case ExpressionType.Constant:
                    return BuildConstantCondition(equality as ConstantExpression, paramaters);
                default:
                    throw new NotImplementedException($"Cannot compile expression: {equality.NodeType} to SQL");
            }
        }

        protected virtual string BuildAndCondition(IEnumerable<(string name, MemberInfo token, ParameterExpression reference)> tables, BinaryExpression and, IList<object> paramaters) =>
            $"({BuildCondition(tables, and.Left, paramaters)} AND {BuildCondition(tables, and.Right, paramaters)})";

        protected virtual string BuildOrCondition(IEnumerable<(string name, MemberInfo token, ParameterExpression reference)> tables, BinaryExpression or, IList<object> paramaters) =>
            $"({BuildCondition(tables, or.Left, paramaters)} OR {BuildCondition(tables, or.Right, paramaters)})";

        protected virtual string BuildEqualityCondition(IEnumerable<(string name, MemberInfo token, ParameterExpression reference)> tables, BinaryExpression eq, IList<object> paramaters) =>
            $"({BuildCondition(tables, eq.Left, paramaters)} = {BuildCondition(tables, eq.Right, paramaters)})";

        protected virtual string BuildNonEqualityCondition(IEnumerable<(string name, MemberInfo token, ParameterExpression reference)> tables, BinaryExpression eq, IList<object> paramaters) =>
            $"({BuildCondition(tables, eq.Left, paramaters)} <> {BuildCondition(tables, eq.Right, paramaters)})";

        protected virtual string BuildLessThanCondition(IEnumerable<(string name, MemberInfo token, ParameterExpression reference)> tables, BinaryExpression eq, IList<object> paramaters) =>
            $"({BuildCondition(tables, eq.Left, paramaters)} < {BuildCondition(tables, eq.Right, paramaters)})";

        protected virtual string BuildLessThanEqualToCondition(IEnumerable<(string name, MemberInfo token, ParameterExpression reference)> tables, BinaryExpression eq, IList<object> paramaters) =>
            $"({BuildCondition(tables, eq.Left, paramaters)} <= {BuildCondition(tables, eq.Right, paramaters)})";

        protected virtual string BuildGreaterThanCondition(IEnumerable<(string name, MemberInfo token, ParameterExpression reference)> tables, BinaryExpression eq, IList<object> paramaters) =>
            $"({BuildCondition(tables, eq.Left, paramaters)} > {BuildCondition(tables, eq.Right, paramaters)})";

        protected virtual string BuildGreaterThanEqualToCondition(IEnumerable<(string name, MemberInfo token, ParameterExpression reference)> tables, BinaryExpression eq, IList<object> paramaters) =>
            $"({BuildCondition(tables, eq.Left, paramaters)} >= {BuildCondition(tables, eq.Right, paramaters)})";

        protected virtual string BuildMemberAccessCondition(IEnumerable<(string name, MemberInfo token, ParameterExpression reference)> tables, MemberExpression member, IList<object> paramaters)
        {
            // e.g. if overall join expression is TQuery.Entity1.Id == Entity2.Id
            // then, "propertyExpression" input will be lhs -or- rhs of above

            // propertyExpression is "TQuery.Entity1.Id" part
            var propertyExpression = member.Expression as MemberExpression;
            if (propertyExpression != null)
            {
                foreach (var table in tables)
                {
                    if (table.token == propertyExpression.Member)
                        return $"{WrapTable(table.name)}.{WrapColumn(member.Member.Name)}";
                }

                throw new InvalidOperationException($"Cannot find table for expression ${member}");
            }

            // propertyExpression is "TQuery.Entity1.Id" part wrapped in "Sql.One" (Sql.One(TQuery.Entity1).Id)
            var oneMember = IsOne(member.Expression);
            if (oneMember != null)
            {
                foreach (var table in tables)
                {
                    if (table.token == oneMember)
                        return $"{WrapTable(table.name)}.{WrapColumn(member.Member.Name)}";
                }

                throw new InvalidOperationException($"Cannot find table for expression ${member}");
            }

            // propertyExpression is "Entity2.Id" part
            var paramExpression = member.Expression as ParameterExpression;
            if (paramExpression != null)
            {
                foreach (var table in tables)
                {
                    if (table.reference == paramExpression)
                        return $"{WrapTable(table.name)}.{WrapColumn(member.Member.Name)}";
                }

                throw new InvalidOperationException($"Cannot find table for expression ${member}");
            }

            throw new InvalidOperationException($"Cannot find table for expression ${member}");
        }

        static readonly MethodInfo _One = typeof(Sql).GetMethod(nameof(Sql.One), BindingFlags.Public | BindingFlags.Static);
        static MemberInfo IsOne(Expression e)
        {
            var method = e as MethodCallExpression;
            if (method == null)
                return null;

            if (!method.Method.IsGenericMethod ||
                method.Method.GetGenericMethodDefinition() != _One)
                return null;

            var input = method.Arguments[0].NodeType == ExpressionType.Convert ? 
                (method.Arguments[0] as UnaryExpression).Operand as MemberExpression :
                method.Arguments[0] as MemberExpression;

            if (input == null)
                return null;

            if (input.Expression is ParameterExpression)
                return input.Member;

            return null;
        }

        protected virtual string BuildConstantCondition(ConstantExpression constant, IList<object> paramaters)
        {
            paramaters.Add(constant.Value);
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
