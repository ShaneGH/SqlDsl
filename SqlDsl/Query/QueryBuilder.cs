using SqlDsl.DataParser;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace SqlDsl.Query
{
    public class QueryBuilder<TSqlBuilder, TResult> : ITable<TResult>, IQuery<TResult>
        where TSqlBuilder: ISqlBuilder, new()
    {
        string PrimaryTable;
        public (string name, Type type)? PrimaryTableMember { get; private set; }

        static (string name, Type Type) CheckMemberExpression<T>(Expression<Func<TResult, T>> memberPointer)
        {
            var body = ReflectionUtils.RemoveConvert(memberPointer.Body);
            if (body == memberPointer.Parameters[0])
                return (SqlBuilderBase.RootObjectAlias, memberPointer.Parameters[0].Type);

            var output = new List<MemberInfo>();
            var expr = TryOne(body) as MemberExpression;
            while (expr != null)
            {
                output.Insert(0, expr.Member);
                expr = TryOne(expr.Expression) as MemberExpression;
            }

            if (!output.Any() || output[0].DeclaringType != typeof(TResult))
                throw new ArgumentException("This expression must point to a paramater on the query object.", nameof(memberPointer));
                
            return (output.MemberName(), GetPropertyOrFieldType(output.Last()));

            Expression TryOne(Expression val) => ReflectionUtils.IsOne(val) ?? val;
        }

        public IQuery<TResult> From<TTable>(string tableName, Expression<Func<TResult, TTable>> tableProperty)
        {
            PrimaryTable = tableName ?? throw new ArgumentNullException(nameof(tableName));
            PrimaryTableMember = CheckMemberExpression(tableProperty);

            return this;
        }

        public IQuery<TResult> From<TTable>(Expression<Func<TResult, TTable>> tableProperty) =>
            From<TTable>(typeof(TTable).Name, tableProperty);

        public IQuery<TResult> From() =>
            From(typeof(TResult).Name);

        public IQuery<TResult> From(string tableName) =>
            From<TResult>(tableName, x => x);

        readonly List<Join> Joins = new List<Join>();
        public IJoinBuilder<TResult, TJoin> InnerJoin<TJoin>(string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinResult) =>
            new JoinBuilder<TJoin>(this, JoinType.Inner, tableName, joinResult);
        
        public IJoinBuilder<TResult, TJoin> InnerJoin<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinResult) =>
            InnerJoin<TJoin>(typeof(TJoin).Name, joinResult);
        
        public IJoinBuilder<TResult, TJoin> LeftJoin<TJoin>(string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinResult) =>
            new JoinBuilder<TJoin>(this, JoinType.Left, tableName, joinResult);
        
        public IJoinBuilder<TResult, TJoin> LeftJoin<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinResult) =>
            LeftJoin<TJoin>(typeof(TJoin).Name, joinResult);

        (ParameterExpression queryRoot, Expression where)? WhereClause = null;
        public IResultMapper<TResult> Where(Expression<Func<TResult, bool>> filter)
        {
            if (filter == null)
                throw new ArgumentNullException(nameof(filter));

            WhereClause = (filter.Parameters[0], filter.Body);
            return this;
        }

        static IEnumerable<string> ColumnsOf(Type t)
        {
            foreach (var col in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                yield return col.Name;
                
            foreach (var col in t.GetFields(BindingFlags.Public | BindingFlags.Instance))
                yield return col.Name;
        }

        static Type GetPropertyOrFieldType(MemberInfo member)
        {
            if (member is PropertyInfo)
                return (member as PropertyInfo).PropertyType;
            if (member is FieldInfo)
                return (member as FieldInfo).FieldType;

            throw new InvalidOperationException("Member must be a property or field: " + member);
        }
        
        public (string sql, IEnumerable<object> paramaters) ToSql()
        {
            return ToSql(null);
        }

        public (string sql, IEnumerable<object> paramaters) ToSql(IEnumerable<string> filterCols)
        {
            var result = ToSqlBuilder(filterCols);
            var sql = result.builder.ToSqlString();
            return ($"{sql.querySetupSql}\n{sql.querySql}", result.paramaters);
        }

        

        internal (ISqlBuilder builder, IEnumerable<object> paramaters) ToSqlBuilder(IEnumerable<string> filterCols)
        {
            if (PrimaryTableMember == null)
                throw new InvalidOperationException("You must set the FROM table before calling ToSql");

            var param = new List<object>();
            var n = Environment.NewLine;
            var builder = new TSqlBuilder();

            builder.SetPrimaryTable(PrimaryTable, PrimaryTableMember.Value.name);

            var selectColumns = Joins
                .SelectMany((x, i) => ColumnsOf(x.JoinExpression.joinParam.Type)
                    .Select(y => (table: x.JoinResult.name, column: y)))
                .Concat(ColumnsOf(PrimaryTableMember.Value.type)
                    .Select(y => (table: PrimaryTableMember.Value.name, column: y)));

            if (filterCols != null)
            {
                var cols = new HashSet<string>(filterCols);
                selectColumns = selectColumns.Where(c => 
                    cols.Contains($"{c.table}.{c.column}"));
            }

            foreach (var col in selectColumns)
            {
                var alias = col.table == SqlBuilderBase.RootObjectAlias ? null : $"{col.table}.{col.column}";
                builder.AddSelectColumn(col.column, col.table, alias);
            }

            foreach (var join in Joins)
            {
                builder.AddJoin(
                    join.JoinType, 
                    join.TableName, 
                    join.JoinExpression.rootObjectParam,
                    join.JoinExpression.joinParam,
                    join.JoinExpression.joinExpression,
                    param,
                    join.JoinResult.name);
            }

            if (WhereClause != null)
                builder.SetWhere(WhereClause.Value.queryRoot, WhereClause.Value.where, param);

            return (builder, param);
        }

        public ISqlBuilder<TMapped> Map<TMapped>(Expression<Func<TResult, TMapped>> mapper) =>
            new QueryMapper<TSqlBuilder, TResult, TMapped>(this, mapper);

        public async Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor) 
        {
            if (PrimaryTableMember == null)
                throw new InvalidOperationException("You must set the FROM table before calling ToSql");

            var sql = ToSql();

            var reader = await executor.ExecuteAsync(sql.sql, sql.paramaters);
            var results = await reader.GetRowsAsync();

            var primaryTableName = PrimaryTableMember.Value.name == SqlBuilderBase.RootObjectAlias ?
                null :
                PrimaryTableMember.Value.name;

            return results.Parse<TResult>(primaryTableName);
        }

        class JoinBuilder<TJoin> : IJoinBuilder<TResult, TJoin>
        {
            readonly QueryBuilder<TSqlBuilder, TResult> Query;
            readonly JoinType JoinType;
            readonly string TableName;
            readonly Expression<Func<TResult, IEnumerable<TJoin>>> JoinResult;

            public JoinBuilder(QueryBuilder<TSqlBuilder, TResult> query, JoinType joinType, string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinResult)
            {
                Query = query ?? throw new ArgumentNullException(nameof(query));
                JoinType = joinType;
                JoinResult = joinResult ?? throw new ArgumentNullException(nameof(joinResult));
                TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            }

            public IQuery<TResult> On(Expression<Func<TResult, TJoin, bool>> joinExpression)
            {                    
                if (joinExpression == null)
                    throw new ArgumentNullException(nameof(joinExpression));

                Query.Joins.Add(new Join(
                    JoinType,
                    TableName,
                    (joinExpression.Parameters[0], joinExpression.Parameters[1], joinExpression.Body),
                    CheckMemberExpression(JoinResult)));

                return Query;
            }
        }
    }
}
