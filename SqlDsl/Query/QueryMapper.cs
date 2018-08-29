using SqlDsl.DataParser;
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
        where TSqlBuilder : ISqlBuilder, new()
    {
        readonly QueryBuilder<TSqlBuilder, TResult> Query;
        readonly IEnumerable<(string from, int toIndex, Type toType)> Mapper;

        public QueryMapper(QueryBuilder<TSqlBuilder, TResult> query, Expression<Func<TResult, TMapped>> mapper)
        {
            Query = query ?? throw new ArgumentNullException(nameof(query));
            Mapper = BuildMap(mapper ?? throw new ArgumentNullException(nameof(mapper)));
        }

        public (string sql, IEnumerable<object> paramaters) ToSql()
        {
            var wrappedSql = Query.ToSqlBuilder(Mapper.Select(m => m.from));
            var builder = new TSqlBuilder();
            builder.SetPrimaryTable(wrappedSql.builder, wrappedSql.builder.InnerQueryAlias);

            foreach (var col in Mapper)
                builder.AddSelectColumn(col.from, tableName: wrappedSql.builder.InnerQueryAlias, alias: $"out{col.toIndex}");
            
            var sql = builder.ToSqlString();
            return ($"{sql.querySetupSql}\n{sql.querySql}", wrappedSql.paramaters);
        }
        
        public async Task<IEnumerable<TMapped>> ExecuteAsync(IExecutor executor)
        {
            var sql = ToSql();

            var reader = await executor.ExecuteAsync(sql.sql, sql.paramaters);
            var results = await reader.GetRowsAsync();

            return results.Parse<TMapped>(Query.PrimaryTableMember.Name);
        }

        IEnumerable<MemberExpression> FindMapExpressions(Expression expr, ParameterExpression rootParam)
        {
            switch (expr.NodeType)
            {
                case ExpressionType.MemberAccess:
                    var memberExpr = expr as MemberExpression;
                    return IsMemberAccessMapped(memberExpr, rootParam) ?
                        new [] { memberExpr } :
                        Enumerable.Empty<MemberExpression>();
                case ExpressionType.Block:
                    return (expr as BlockExpression).Expressions
                        .SelectMany(ex => FindMapExpressions(ex, rootParam));
                case ExpressionType.New:
                    return (expr as NewExpression).Arguments
                        .SelectMany(ex => FindMapExpressions(ex, rootParam));
                case ExpressionType.MemberInit:
                    var init = expr as MemberInitExpression;
                    return FindMapExpressions(init.NewExpression, rootParam)
                        .Concat(init.Bindings
                            .OfType<MemberAssignment>()
                            .SelectMany(b => FindMapExpressions(b.Expression, rootParam)));
            }

            throw new InvalidOperationException($"Unsupported mapping expression \"{expr}\".");
        }

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

        IEnumerable<(string from, int toIndex, Type toType)> BuildMap(Expression<Func<TResult, TMapped>> mapper)
        {
            return FindMapExpressions(mapper.Body, mapper.Parameters[0])
                .Select((e, i) => (CompileMemberName(e), i, e.Type))
                .Enumerate();
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
