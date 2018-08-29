using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using SqlDsl.DataParser;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.Query
{
    public class QueryBuilder<TSqlBuilder, TResult> : ITable<TResult>, IQuery<TResult>
        where TSqlBuilder: ISqlBuilder, new()
        where TResult: new()
    {
        string PrimaryTable;
        public MemberInfo PrimaryTableMember { get; private set; }

        static MemberInfo CheckMemberExpression<T>(Expression<Func<TResult, T>> memberPointer)
        {
            var expr = memberPointer.Body as MemberExpression;
            if (expr == null || expr.Member.DeclaringType != typeof(TResult))
                throw new ArgumentException("This expression must point to a paramater on the query object.", nameof(memberPointer));
                
            return expr.Member;
        }

        public IQuery<TResult> From<TTable>(string tableName, Expression<Func<TResult, TTable>> tableProperty)
        {
            PrimaryTable = tableName ?? throw new ArgumentNullException(nameof(tableName));
            PrimaryTableMember = CheckMemberExpression(tableProperty);

            return this;
        }

        readonly List<Join> Joins = new List<Join>();
        public IJoinBuilder<TResult, TJoin> InnerJoin<TJoin>(string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinResult)
        {
            return new JoinBuilder<TJoin>(this, JoinType.Inner, tableName, joinResult);
        }
        
        public IJoinBuilder<TResult, TJoin> LeftJoin<TJoin>(string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinResult)
        {
            return new JoinBuilder<TJoin>(this, JoinType.Left, tableName, joinResult);
        }

        Expression WhereClause = null;
        public IResultMapper<TResult> Where(Expression<Func<TResult, bool>> filter)
        {
            WhereClause = filter?.Body ?? throw new ArgumentNullException(nameof(filter));
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
            var param = new List<object>();
            var n = Environment.NewLine;
            var builder = new TSqlBuilder();

            builder.SetPrimaryTable(PrimaryTable, PrimaryTableMember.Name);

            var selectColumns = Joins
                .SelectMany((x, i) => ColumnsOf(x.JoinExpression.joinParam.Type)
                    .Select(y => (table: x.JoinResult.Name, column: y)))
                .Concat(ColumnsOf(GetPropertyOrFieldType(PrimaryTableMember))
                    .Select(y => (table: PrimaryTableMember.Name, column: y)));

            if (filterCols != null)
            {
                var cols = new HashSet<string>(filterCols);
                selectColumns = selectColumns.Where(c => 
                    cols.Contains($"{c.table}.{c.column}"));
            }

            foreach (var col in selectColumns)
                builder.AddSelectColumn(col.column, col.table, $"{col.table}.{col.column}");

            var tables = Joins
                .Select(x => (x.JoinResult.Name, x.JoinResult, x.JoinExpression.joinParam))
                .Prepend((PrimaryTableMember.Name, PrimaryTableMember, null))
                .Enumerate();

            foreach (var join in Joins)
            {
                builder.AddJoin(
                    join.JoinType, 
                    join.TableName, 
                    (tables, join.JoinExpression.joinExpression, param),
                    join.JoinResult.Name);
            }

            if (WhereClause != null)
                builder.SetWhere(tables, WhereClause, param);

            return (builder, param);
        }

        public ISqlBuilder<TMapped> Map<TMapped>(Expression<Func<TResult, TMapped>> mapper) 
            where TMapped : new() => new QueryMapper<TSqlBuilder, TResult, TMapped>(this, mapper);

        public async Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor) 
        {
            var sql = ToSql();

            var reader = await executor.ExecuteAsync(sql.sql, sql.paramaters);
            var results = await reader.GetRowsAsync();

            return results.Parse<TResult>(PrimaryTableMember.Name);
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

                Query.Joins.Add(new Join
                {
                    JoinType = JoinType,
                    TableName = TableName,
                    JoinExpression = (joinExpression.Parameters[1], joinExpression.Body),
                    JoinResult = CheckMemberExpression(JoinResult)
                });

                return Query;
            }
        }
    }
}
