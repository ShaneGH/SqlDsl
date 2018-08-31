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
        public IEnumerable<MemberInfo> PrimaryTableMember { get; private set; }

        static IEnumerable<MemberInfo> CheckMemberExpression<T>(Expression<Func<TResult, T>> memberPointer)
        {
            var output = new List<MemberInfo>();
            var expr = memberPointer.Body as MemberExpression;
            while (expr != null)
            {
                output.Insert(0, expr.Member);
                expr = expr.Expression as MemberExpression;
            }

            if (!output.Any() || output[0].DeclaringType != typeof(TResult))
                throw new ArgumentException("This expression must point to a paramater on the query object.", nameof(memberPointer));
                
            return output;
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
            var param = new List<object>();
            var n = Environment.NewLine;
            var builder = new TSqlBuilder();

            var primaryTableName = PrimaryTableMember.MemberName();
            builder.SetPrimaryTable(PrimaryTable, primaryTableName);

            var selectColumns = Joins
                .SelectMany((x, i) => ColumnsOf(x.JoinExpression.joinParam.Type)
                    .Select(y => (table: x.JoinResult.MemberName(), column: y)))
                .Concat(ColumnsOf(GetPropertyOrFieldType(PrimaryTableMember.Last()))
                    .Select(y => (table: primaryTableName, column: y)));

            if (filterCols != null)
            {
                var cols = new HashSet<string>(filterCols);
                selectColumns = selectColumns.Where(c => 
                    cols.Contains($"{c.table}.{c.column}"));
            }

            foreach (var col in selectColumns)
                builder.AddSelectColumn(col.column, col.table, $"{col.table}.{col.column}");

            var tables = Joins
                .Select(x => (x.JoinResult.MemberName(), x.JoinResult, x.JoinExpression.joinParam))
                .Prepend((primaryTableName, PrimaryTableMember, null))
                .Enumerate();

            foreach (var join in Joins)
            {
                builder.AddJoin(
                    join.JoinType, 
                    join.TableName, 
                    join.JoinExpression.rootObjectParam,
                    join.JoinExpression.joinParam,
                    join.JoinExpression.joinExpression,
                    param,
                    join.JoinResult.MemberName());
            }

            if (WhereClause != null)
                builder.SetWhere(WhereClause.Value.queryRoot, WhereClause.Value.where, param);

            return (builder, param);
        }

        public ISqlBuilder<TMapped> Map<TMapped>(Expression<Func<TResult, TMapped>> mapper) =>
            new QueryMapper<TSqlBuilder, TResult, TMapped>(this, mapper);

        public async Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor) 
        {
            var sql = ToSql();

            var reader = await executor.ExecuteAsync(sql.sql, sql.paramaters);
            var results = await reader.GetRowsAsync();

            return results
                .Parse<TResult>(PrimaryTableMember.MemberName());
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
                    JoinExpression = (joinExpression.Parameters[0], joinExpression.Parameters[1], joinExpression.Body),
                    JoinResult = CheckMemberExpression(JoinResult)
                });

                return Query;
            }
        }
    }
}
