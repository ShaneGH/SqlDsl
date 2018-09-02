using SqlDsl.DataParser;
using SqlDsl.Dsl;
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
        where TSqlBuilder: ISqlFragmentBuilder, new()
    {
        string PrimaryTable;
        public (string name, Type type)? PrimaryTableMember { get; private set; }

        static (string name, Type Type) CheckMemberExpression(Expression body, ParameterExpression queryParameter)
        {
            body = ReflectionUtils.RemoveConvert(body);
            if (body == queryParameter)
                return (SqlStatementConstants.RootObjectAlias, queryParameter.Type);

            var output = new List<MemberInfo>();
            var expr = TryOne(body) as MemberExpression;
            while (expr != null)
            {
                output.Insert(0, expr.Member);
                expr = TryOne(expr.Expression) as MemberExpression;
            }

            if (!output.Any() || output[0].DeclaringType != typeof(TResult))
                throw new ArgumentException("This expression must point to a paramater on the query object.", nameof(body));
                
            return (output.MemberName(), GetPropertyOrFieldType(output.Last()));

            Expression TryOne(Expression val) => ReflectionUtils.IsOne(val) ?? val;
        }

        public IQuery<TResult> From<TTable>(string tableName, Expression<Func<TResult, TTable>> tableProperty)
        {
            PrimaryTable = tableName ?? throw new ArgumentNullException(nameof(tableName));
            PrimaryTableMember = CheckMemberExpression(tableProperty.Body, tableProperty.Parameters[0]);

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
            
        public IJoinBuilder<TResult, TJoin> InnerJoin<TJoin>(string tableName, Expression<Func<TResult, TJoin>> joinResult) =>
            new JoinBuilder<TJoin>(this, JoinType.Inner, tableName, joinResult);
        
        public IJoinBuilder<TResult, TJoin> InnerJoin<TJoin>(Expression<Func<TResult, TJoin>> joinResult) =>
            InnerJoin<TJoin>(typeof(TJoin).Name, joinResult);
        
        public IJoinBuilder<TResult, TJoin> LeftJoin<TJoin>(string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinResult) =>
            new JoinBuilder<TJoin>(this, JoinType.Left, tableName, joinResult);
        
        public IJoinBuilder<TResult, TJoin> LeftJoin<TJoin>(Expression<Func<TResult, IEnumerable<TJoin>>> joinResult) =>
            LeftJoin<TJoin>(typeof(TJoin).Name, joinResult);
        
        public IJoinBuilder<TResult, TJoin> LeftJoin<TJoin>(string tableName, Expression<Func<TResult, TJoin>> joinResult) =>
            new JoinBuilder<TJoin>(this, JoinType.Left, tableName, joinResult);
        
        public IJoinBuilder<TResult, TJoin> LeftJoin<TJoin>(Expression<Func<TResult, TJoin>> joinResult) =>
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
            return ToSql(filterCols: null);
        }

        public (string sql, IEnumerable<object> paramaters) ToSql(IEnumerable<string> filterCols)
        {
            var result = ToSqlBuilder(filterCols);
            var sql = result.builder.ToSqlString();
            return (ToSql(result.builder), result.paramaters);
        }

        string ToSql(ISqlBuilderOLD builder)
        {
            var sql = builder.ToSqlString();
            return $"{sql.querySetupSql}\n\n{sql.querySql}";
        }        

        internal (ISqlBuilderOLD builder, IEnumerable<object> paramaters) ToSqlBuilder(IEnumerable<string> filterSelectCols)
        {
            if (PrimaryTableMember == null)
                throw new InvalidOperationException("You must set the FROM table before calling ToSql");

            var param = new List<object>();
            var n = Environment.NewLine;
            var builder = new SqlStatementBuilder<TSqlBuilder>();

            builder.SetPrimaryTable(PrimaryTable, PrimaryTableMember.Value.name);

            var selectColumns = Joins
                .SelectMany((x, i) => ColumnsOf(x.JoinExpression.joinParam.Type)
                    .Select(y => (table: x.JoinedTableProperty.name, column: y)))
                .Concat(ColumnsOf(PrimaryTableMember.Value.type)
                    .Select(y => (table: PrimaryTableMember.Value.name, column: y)));

            if (filterSelectCols != null)
            {
                var cols = new HashSet<string>(filterSelectCols);
                selectColumns = selectColumns.Where(c => c.table == SqlStatementConstants.RootObjectAlias ?
                    cols.Contains(c.column) :
                    cols.Contains($"{c.table}.{c.column}"));
            }

            foreach (var col in selectColumns)
            {
                var alias = col.table == SqlStatementConstants.RootObjectAlias ? null : $"{col.table}.{col.column}";
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
                    join.JoinedTableProperty.name);
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

            var sqlBuilder = ToSqlBuilder(null);
            var sql = ToSql(sqlBuilder.builder);

            var reader = await executor.ExecuteAsync(sql, sqlBuilder.paramaters);
            var results = await reader.GetRowsAsync();

            var primaryTableName = PrimaryTableMember.Value.name == SqlStatementConstants.RootObjectAlias ?
                null :
                PrimaryTableMember.Value.name;

            var selectColumns = sqlBuilder.builder.SelectColumns.ToArray();
            var rowIdColumns = sqlBuilder.builder.RowIdMap.ToList();
            var rowIdMap = selectColumns
                .Select(c => 
                {
                    var op = rowIdColumns
                        .Where(rid => rid.columnName == c)
                        .Select(rid => rid.rowIdColumnName)
                        .FirstOrDefault() ??
                        throw new InvalidOperationException($"Cannot find row id for column {c}");

                    var index = selectColumns.IndexOf(op);

                    if (index == -1) throw new InvalidOperationException($"Cannot find row id for column {c}");

                    return index;
                })
                .ToArray();

            // TODO: compile and cache ObjectProperty graph, and use as first arg
            return results.Parse<TResult>(selectColumns, rowIdMap, primaryTableName);
        }

        class JoinBuilder<TJoin> : IJoinBuilder<TResult, TJoin>
        {
            readonly QueryBuilder<TSqlBuilder, TResult> Query;
            readonly JoinType JoinType;
            readonly string TableName;
            readonly Expression JoinResultBody;
            readonly ParameterExpression JoinResultQueryParam;

            private JoinBuilder(
                QueryBuilder<TSqlBuilder, TResult> query, 
                JoinType joinType, 
                string tableName, 
                Expression joinResultBody, 
                ParameterExpression joinResultQueryParam)
            {
                Query = query ?? throw new ArgumentNullException(nameof(query));
                JoinType = joinType;
                JoinResultBody = joinResultBody ?? throw new ArgumentNullException(nameof(joinResultBody));
                JoinResultQueryParam = joinResultQueryParam ?? throw new ArgumentNullException(nameof(joinResultQueryParam));
                TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            }

            public JoinBuilder(QueryBuilder<TSqlBuilder, TResult> query, JoinType joinType, string tableName, Expression<Func<TResult, TJoin>> joinResult)
                : this(query, joinType, tableName, joinResult?.Body, joinResult?.Parameters[0])
            {
            }

            public JoinBuilder(QueryBuilder<TSqlBuilder, TResult> query, JoinType joinType, string tableName, Expression<Func<TResult, IEnumerable<TJoin>>> joinResult)
                : this(query, joinType, tableName, joinResult?.Body, joinResult?.Parameters[0])
            {
            }
            
            public IQuery<TResult> On(Expression<Func<TResult, TJoin, bool>> joinExpression)
            {                    
                if (joinExpression == null)
                    throw new ArgumentNullException(nameof(joinExpression));

                Query.Joins.Add(new Join(
                    JoinType,
                    TableName,
                    (joinExpression.Parameters[0], joinExpression.Parameters[1], joinExpression.Body),
                    CheckMemberExpression(JoinResultBody, JoinResultQueryParam)));

                return Query;
            }
        }
    }
}
