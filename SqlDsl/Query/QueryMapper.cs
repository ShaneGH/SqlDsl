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
        readonly IEnumerable<(string from, string to)> MappedValues;

        public QueryMapper(QueryBuilder<TSqlBuilder, TResult> query, Expression<Func<TResult, TMapped>> mapper)
        {
            Query = query ?? throw new ArgumentNullException(nameof(query));
            MappedValues = BuildMap(
                new BuildMapState(mapper.Parameters[0]),
                mapper?.Body ?? throw new ArgumentNullException(nameof(mapper)),
                mapper.Parameters[0])
                .Select(x => (RemoveRoot(x.from), RemoveRoot(x.to)))
                .Enumerate();
        }

        public (string sql, IEnumerable<object> paramaters) ToSql()
        {
            var result = ToSqlBuilder();
            var sql = result.builder.ToSqlString();
            return (result.builder.ToSql(), result.paramaters);
        }

        (ISqlStatement builder, IEnumerable<object> paramaters) ToSqlBuilder()
        {
            var wrappedSql = Query.ToSqlBuilder(MappedValues.Select(m => m.from));
            var builder = new SqlStatementBuilder<TSqlBuilder>();
            builder.SetPrimaryTable(wrappedSql.builder, wrappedSql.builder.UniqueAlias);

            foreach (var col in MappedValues)
                builder.AddSelectColumn(col.from, tableName: wrappedSql.builder.UniqueAlias, alias: col.to);
            
            var sql = builder.ToSqlString();

            return (builder, wrappedSql.paramaters);
        }
        
        public Task<IEnumerable<TMapped>> ExecuteAsync(IExecutor executor)
        {       
            if (Query.PrimaryTableMember == null)
                throw new InvalidOperationException("You must set the FROM table before calling ToSql");

            var sqlBuilder = ToSqlBuilder();
            return executor.ExecuteAsync<TMapped>(sqlBuilder.builder, sqlBuilder.paramaters, Query.PrimaryTableMember.Value.name);
        }

        class BuildMapState
        {
            public readonly ParameterExpression QueryObject;
            public readonly List<(ParameterExpression param, string queryName)> ParameterMap = new List<(ParameterExpression, string)>();

            public BuildMapState(ParameterExpression queryObject)
            {
                QueryObject = queryObject;
            }
        }

        IEnumerable<(string from, string to)> BuildMap(BuildMapState state, Expression expr, ParameterExpression rootParam, string toPrefix = null)
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
                        .SelectMany(ex => BuildMap(state, ex, rootParam, toPrefix));
                case ExpressionType.New:
                    return (expr as NewExpression).Arguments
                        .SelectMany(ex => BuildMap(state, ex, rootParam, toPrefix));
                case ExpressionType.MemberInit:
                    var init = expr as MemberInitExpression;
                    return BuildMap(state, init.NewExpression, rootParam, toPrefix)
                        .Concat(init.Bindings
                            .OfType<MemberAssignment>()
                            .SelectMany(b => BuildMap(state, b.Expression, rootParam, b.Member.Name)
                                .Select(x => (x.from, CombineStrings(toPrefix, x.to)))));
                case ExpressionType.Call:
                    var toList = ReflectionUtils.IsToList(expr as MethodCallExpression);
                    if (toList.isToList)
                        return BuildMap(state, toList.enumerable, rootParam, toPrefix);
                        
                    var toArray = ReflectionUtils.IsToArray(expr as MethodCallExpression);
                    if (toArray.isToArray)
                        return BuildMap(state, toArray.enumerable, rootParam, toPrefix);

                    var callExpr = ReflectionUtils.IsSelectWithLambdaExpression(expr as MethodCallExpression);
                    if (callExpr.isSelect)
                        return BuildMapForSelect(state, callExpr.enumerable, callExpr.mapper, rootParam, toPrefix);

                    var joinExpr = ReflectionUtils.IsJoined(expr as MethodCallExpression);
                    if (joinExpr.isJoined)
                        return BuildMapForJoined(state, joinExpr.joinedFrom, joinExpr.joinedTo, rootParam, toPrefix);

                    break;
            }

            throw new InvalidOperationException($"Unsupported mapping expression \"{expr}\".");
        }

        IEnumerable<(string from, string to)> BuildMapForSelect(BuildMapState state, Expression enumerable, LambdaExpression mapper, ParameterExpression rootParam, string toPrefix)
        {
            var innerMap = BuildMap(state, mapper.Body, mapper.Parameters[0]);
            var rootMap = BuildMap(state, enumerable, rootParam, toPrefix).Enumerate();
            
            // add a map from this mapper.Parameters[0] to it's query alias
            int tmp;
            if ((tmp = rootMap.Count()) > 1) 
            {
                throw new InvalidOperationException("Expecting zero or one result. Cannot map multiple results to one parameter.");
            } 
            else if (tmp == 1)
            {
                state.ParameterMap.Add((mapper.Parameters[0], rootMap.First().from));
            }

            return rootMap
                .SelectMany(r => innerMap
                    .Select(m => (CombineStrings(r.from, m.from), CombineStrings(r.to, m.to))));
        }

        IEnumerable<(string from, string to)> BuildMapForJoined(BuildMapState state, Expression joinedFrom, Expression joinedTo, ParameterExpression rootParam, string toPrefix)
        {
            return BuildMap(state, joinedTo, state.QueryObject, toPrefix)
                .Select(x => ($"{SqlStatementConstants.RootObjectAlias}.{x.from}", x.to));
        }

        static readonly string RootObjectAsPrefix = $"{SqlStatementConstants.RootObjectAlias}.";

        static string CombineStrings(string s1, string s2)
        {
            if (s2 != null && s2.StartsWith(RootObjectAsPrefix))
                return s2;

            return s1 == null && s2 == null ?
                null :
                s1 != null && s2 != null ? 
                    $"{s1}.{s2}" :
                    $"{s1}{s2}";
        }

        static string RemoveRoot(string s) =>
            s != null && s.StartsWith(RootObjectAsPrefix) ?
                s.Substring(RootObjectAsPrefix.Length) :
                s;

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
