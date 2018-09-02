using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Query
{
    public class QueryMapper<TSqlBuilder, TResult, TMapped> : ISqlBuilder<TMapped>
        where TSqlBuilder: ISqlFragmentBuilder, new()
    {
        readonly QueryBuilder<TSqlBuilder, TResult> Query;
        readonly IEnumerable<(string from, string to)> Mapper;

        public QueryMapper(QueryBuilder<TSqlBuilder, TResult> query, Expression<Func<TResult, TMapped>> mapper)
        {
            Query = query ?? throw new ArgumentNullException(nameof(query));
            Mapper = BuildMap(
                mapper?.Body ?? throw new ArgumentNullException(nameof(mapper)),
                mapper.Parameters[0]);
        }

        public (string sql, IEnumerable<object> paramaters) ToSql()
        {
            var builder = ToSqlBuilder();
            return (ToSql(builder.builder), builder.paramaters);
        }

        string ToSql(ISqlBuilderOLD builder)
        {            
            var sql = builder.ToSqlString();
            return $"{sql.querySetupSql}\n\n{sql.querySql}";
        }

        (ISqlBuilderOLD builder, IEnumerable<object> paramaters) ToSqlBuilder()
        {
            var wrappedSql = Query.ToSqlBuilder(Mapper.Select(m => m.from));
            var builder = new SqlStatementBuilder<TSqlBuilder>();
            builder.SetPrimaryTable(wrappedSql.builder, wrappedSql.builder.InnerQueryAlias);

            foreach (var col in Mapper)
                builder.AddSelectColumn(col.from, tableName: wrappedSql.builder.InnerQueryAlias, alias: col.to);
            
            var sql = builder.ToSqlString();

            return (builder, wrappedSql.paramaters);
        }
        
        public async Task<IEnumerable<TMapped>> ExecuteAsync(IExecutor executor)
        {
            if (Query.PrimaryTableMember == null)
                throw new InvalidOperationException("You must set the FROM table before calling ToSql");

            var sqlBuilder = ToSqlBuilder();
            var sql = ToSql(sqlBuilder.builder);

            var reader = await executor.ExecuteAsync(sql, sqlBuilder.paramaters);
            var results = await reader.GetRowsAsync();

            var tableName = Query.PrimaryTableMember.Value.name == SqlStatementConstants.RootObjectAlias ?
                null :
                Query.PrimaryTableMember.Value.name;

            //TODO: copy pasted from QueryBuilder.ExecuteAsync
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
            return results.Parse<TMapped>(sqlBuilder.builder.SelectColumns, rowIdMap, tableName);
        }

        IEnumerable<(string from, string to)> BuildMap(Expression expr, ParameterExpression rootParam, string toPrefix = null)
        {
            switch (expr.NodeType)
            {
                case ExpressionType.MemberAccess:
                    var memberExpr = expr as MemberExpression;
                    return IsMemberAccessMapped(memberExpr, rootParam) ?
                        new [] { (CompileMemberName(memberExpr), toPrefix) } :
                        Enumerable.Empty<(string, string)>();
                case ExpressionType.Block:
                    return (expr as BlockExpression).Expressions
                        .SelectMany(ex => BuildMap(ex, rootParam, toPrefix));
                case ExpressionType.New:
                    return (expr as NewExpression).Arguments
                        .SelectMany(ex => BuildMap(ex, rootParam, toPrefix));
                case ExpressionType.MemberInit:
                    var init = expr as MemberInitExpression;
                    return BuildMap(init.NewExpression, rootParam, toPrefix)
                        .Concat(init.Bindings
                            .OfType<MemberAssignment>()
                            .SelectMany(b => BuildMap(b.Expression, rootParam, b.Member.Name)
                                .Select(x => (x.from, CombineStrings(toPrefix, x.to)))));
                case ExpressionType.Call:
                    var toList = ReflectionUtils.IsToList(expr as MethodCallExpression);
                    if (toList.isToList)
                        return BuildMap(toList.enumerable, rootParam, toPrefix);
                        
                    var toArray = ReflectionUtils.IsToArray(expr as MethodCallExpression);
                    if (toArray.isToArray)
                        return BuildMap(toArray.enumerable, rootParam, toPrefix);

                    var callExpr = ReflectionUtils.IsSelectWithLambdaExpression(expr as MethodCallExpression);
                    if (!callExpr.isSelect)
                        break;

                    return BuildMap(callExpr.enumerable, rootParam, toPrefix)
                        .SelectMany(r => BuildMap(callExpr.mapper.Body, callExpr.mapper.Parameters[0])
                            .Select(m => (CombineStrings(r.from, m.from), r.to)));
            }

            string CombineStrings(string s1, string s2) =>
                s1 == null && s2 == null ?
                    null :
                    s1 != null && s2 != null ? 
                        $"{s1}.{s2}" :
                        $"{s1}{s2}";

            throw new InvalidOperationException($"Unsupported mapping expression \"{expr}\".");
        }

        // object BuildCallMap()
        // {
        // }

        bool IsMemberAccessMapped(MemberExpression expr, ParameterExpression rootParam)
        {
            while (expr != null)
            {
                if (expr.Expression == rootParam) return true;
                expr = expr.Expression as MemberExpression;
            }

            return false;
        }

        string CompileMemberName(MemberExpression expr)
        {
            var next = expr.Expression as MemberExpression;
            return next != null ?
                $"{CompileMemberName(next)}.{expr.Member.Name}" :
                expr.Member.Name;
        }

        // static bool IsSelect(MethodCallExpression e) =>
        //     e.Method.IsStatic && e.Method.Name == "Select" && e.Method.DeclaringType == typeof(Enumerable);

        // (ParameterExpression, List<MemberInfo>) BuildExpression(Expression e)
        // {
        //     var props = new List<MemberInfo>();
        //     while (!(e is ParameterExpression))
        //     {
        //         switch (e.NodeType)
        //         {
        //             case ExpressionType.Convert:
        //                 e = (e as UnaryExpression).Operand;
        //                 continue;
        //             case ExpressionType.MemberAccess:
        //                 var m = e as MemberExpression;
        //                 props.Insert(0, m.Member);
        //                 e = m.Expression;
        //                 continue;
        //             case ExpressionType.Call:
        //                 var c = e as MethodCallExpression;
        //                 if (!IsSelect(c))
        //                     return (null, null);

        //                 // TODO: dynamic is slow
        //                 Expression body = ((dynamic)c.Arguments[1]).Body;
        //                 if (body == null)
        //                     return (null, null);
                        
        //                 if (body.NodeType == ExpressionType.Convert)
        //                     body = (body as UnaryExpression).Operand;

        //                 if (body.NodeType != ExpressionType.MemberAccess)
        //                     return (null, null);

        //                 props.Insert(0, (body as MemberExpression).Member);
                            
        //                 // .Select(...) is an extension method
        //                 e = c.Arguments[0];
        //                 continue;
        //             default:
        //                 return (null, null);
        //         }
        //     }

        //     return (e as ParameterExpression, props);
        // }
    }
}
