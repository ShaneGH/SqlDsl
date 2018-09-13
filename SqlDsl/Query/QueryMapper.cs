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
        readonly IEnumerable<(string from, string to)> MappedValuesX;
        readonly IEnumerable<(string rowIdColumnName, string resultClassProperty)> RowIdPropertyMapX;
        readonly Expression<Func<TResult, TMapped>> Mapper;


        public QueryMapper(QueryBuilder<TSqlBuilder, TResult> query, Expression<Func<TResult, TMapped>> mapper)
        {
            Query = query ?? throw new ArgumentNullException(nameof(query));
            Mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        }

        public (string sql, IEnumerable<object> paramaters) ToSql()
        {
            var result = ToSqlBuilder();
            var sql = result.builder.ToSqlString();
            return (result.builder.ToSql(), result.paramaters);
        }

        (ISqlStatement builder, IEnumerable<object> paramaters) ToSqlBuilder()
        {
            // var wrappedSql = Query.ToSqlBuilder(MappedValues.Select(m => m.from));
            var (wrappedBuilder, parameters) = Query.ToSqlBuilder(null);

            var map = BuildMap(
                new BuildMapState(Mapper.Parameters[0], null),
                Mapper.Body,
                Mapper.Parameters[0]);

            var rowIdPropertyMap = map.tables
                .Select(t => (rowIdColumnName: $"{t.From}.{SqlStatementConstants.RowIdName}", resultClassProperty: t.To))
                .Enumerate();

            var mappedValues = map.properties
                .Select(x => (from: RemoveRoot(x.From), to: RemoveRoot(x.To)))
                .Enumerate();

            var builder = new SqlStatementBuilder<TSqlBuilder>();
            builder.SetPrimaryTable(wrappedBuilder, wrappedBuilder.UniqueAlias);

            foreach (var col in mappedValues)
                builder.AddSelectColumn(col.from, tableName: wrappedBuilder.UniqueAlias, alias: col.to);

            foreach (var col in rowIdPropertyMap)
                builder.RowIdsForMappedProperties.Add((col.rowIdColumnName, col.resultClassProperty));
            
            var sql = builder.ToSqlString();

            return (builder, parameters);
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
            public readonly List<(ParameterExpression parameter, IEnumerable<string> property)> ParameterRepresentsProperty = new List<(ParameterExpression, IEnumerable<string>)>();
            public readonly List<Mapped> ValidJoins = new List<Mapped>();

            public BuildMapState(ParameterExpression queryObject, IEnumerable<Mapped> validJoins)
            {
                QueryObject = queryObject;
                ValidJoins = validJoins.OrEmpty().ToList(); 
            }
        }

        static readonly IEnumerable<Mapped> EmptyMapped = Enumerable.Empty<Mapped>();

        static (IEnumerable<Mapped> properties, IEnumerable<Mapped> tables) BuildMap(BuildMapState state, Expression expr, ParameterExpression rootParam, string toPrefix = null)
        {
            switch (expr.NodeType)
            {
                case ExpressionType.MemberAccess:
                    var memberExpr = expr as MemberExpression;
                    return IsMemberAccessMapped(memberExpr, rootParam) ?
                        (new [] { new Mapped(CompileMemberName(memberExpr), toPrefix) }, EmptyMapped) :
                        (EmptyMapped, EmptyMapped);

                case ExpressionType.Block:
                    return (expr as BlockExpression).Expressions
                        .Select(ex => BuildMap(state, ex, rootParam, toPrefix))
                        .AggregateTuple2();

                case ExpressionType.New:
                    return (expr as NewExpression).Arguments
                        .Select(ex => BuildMap(state, ex, rootParam, toPrefix))
                        .AggregateTuple2();

                case ExpressionType.MemberInit:
                    var init = expr as MemberInitExpression;
                    return BuildMap(state, init.NewExpression, rootParam, toPrefix)
                        .ToEnumerableStruct()
                        .Concat(init.Bindings
                            .OfType<MemberAssignment>()
                            .Select(b => (memberName: b.Member.Name, map: BuildMap(state, b.Expression, rootParam, b.Member.Name)))
                            .Select(m => (
                                m.map.properties.Select(x => new Mapped(x.From, CombineStrings(toPrefix, x.To))),
                                m.map.tables.Select(x => new Mapped(x.From, CombineStrings(m.memberName, x.To))))))
                        .AggregateTuple2();

                case ExpressionType.Call:
                    var (isToList, enumerableL) = ReflectionUtils.IsToList(expr as MethodCallExpression);
                    if (isToList)
                        return BuildMap(state, enumerableL, rootParam, toPrefix);
                        
                    var (isToArray, enumerableA) = ReflectionUtils.IsToArray(expr as MethodCallExpression);
                    if (isToArray)
                        return BuildMap(state, enumerableA, rootParam, toPrefix);

                    var (isSelect, enumerableS, mapper) = ReflectionUtils.IsSelectWithLambdaExpression(expr as MethodCallExpression);
                    if (isSelect)
                        return BuildMapForSelect(state, enumerableS, mapper, rootParam, toPrefix);

                    var (isJoined, joinedFrom, joinedTo) = ReflectionUtils.IsJoined(expr as MethodCallExpression);
                    if (isJoined)
                        return BuildMapForJoined(state, joinedFrom, joinedTo, rootParam, toPrefix);

                    break;
            }

            throw new InvalidOperationException($"Unsupported mapping expression \"{expr}\".");
        }

        static (IEnumerable<Mapped> properties, IEnumerable<Mapped> tables) BuildMapForSelect(BuildMapState state, Expression enumerable, LambdaExpression mapper, ParameterExpression rootParam, string toPrefix)
        {
            var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(enumerable);
            if (isPropertyChain && root == state.QueryObject)
                state.ParameterRepresentsProperty.Add((mapper.Parameters[0], chain));

            var outerMap = BuildMap(state, enumerable, rootParam, toPrefix);
            var innerMap = BuildMap(state, mapper.Body, mapper.Parameters[0]);
            var outerMapProperties  = outerMap.properties.Enumerate();
            
            var newTableMap = enumerable is MemberExpression ?
                new Mapped(CompileMemberName(enumerable as MemberExpression), null).ToEnumerable() :
                EmptyMapped;

            return (
                outerMapProperties
                    .SelectMany(r => innerMap.properties
                        .Select(m => new Mapped(CombineStrings(r.From, m.From), CombineStrings(r.To, m.To)))),
                outerMap.tables
                    .Concat(innerMap.tables)
                    .Concat(newTableMap)
            );
        }

        /// <summary>
        /// Verify whether a call to Sql.Joined is using parameters which represent an actual join
        /// </summary>
        static void VerifyJoin(BuildMapState state, Expression from, string to)
        {
            switch (from.NodeType)
            {
                case ExpressionType.Call:
                    var (isJoined, joinedFrom, joinedTo) = ReflectionUtils.IsJoined(from as MethodCallExpression);
                    if (!isJoined)
                        throw new InvalidOperationException("Property joined from is invalid");
                        // TODO: better error message

                    var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(joinedTo);
                    if (!isPropertyChain)
                        throw new InvalidOperationException("Property joined from is invalid");
                        // TODO: better error message

                    VerifyJoin(state, joinedFrom, chain.JoinString("."));
                    break;
                case ExpressionType.Parameter:
                    var from_ = state.ParameterRepresentsProperty
                        .Where(s => s.parameter == from)
                        .Select(s => s.property.JoinString("."))
                        .FirstOrDefault();

                    if (from_ == null)
                        throw new InvalidOperationException("Property joined from is invalid");
                        // TODO: better error message

                    VerifyJoin(from_, to);

                    break;
                default:
                    throw new InvalidOperationException("Property joined from is invalid");
                    // TODO: better error message
            }
        }

        /// <summary>
        /// Verify whether a join is valid. Throw an exception if not
        /// </summary>
        static void VerifyJoin(BuildMapState state, string from, string to)
        {
            if (!state.ValidJoins.Any(j => 
                (j.From == from && j.To == to) ||
                (j.From == to && j.To == from)))

                throw new InvalidOperationException($"Property \"{from}\" does not join to property \"{to}\".");
        }

        static (IEnumerable<Mapped> properties, IEnumerable<Mapped> tables) BuildMapForJoined(BuildMapState state, Expression joinedFrom, Expression joinedTo, ParameterExpression rootParam, string toPrefix)
        {
            var op = BuildMap(state, joinedTo, state.QueryObject, toPrefix);

            var propsEnumerated = op.properties.ToList();
            if (propsEnumerated.Count != 1)
                throw new InvalidOperationException("A join must contain a reference to 1 table on the query object");
                
            VerifyJoin(state, joinedFrom, propsEnumerated[0].From);

            return (
                propsEnumerated
                    .Select(x => new Mapped($"{SqlStatementConstants.RootObjectAlias}.{x.From}", x.To)),
                op.tables);
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

        static bool IsMemberAccessMapped(MemberExpression expr, ParameterExpression rootParam)
        {
            while (expr != null)
            {
                if (expr.Expression == rootParam) return true;
                expr = expr.Expression as MemberExpression;
            }

            return false;
        }

        static string CompileMemberName(MemberExpression expr)
        {
            var next = expr.Expression as MemberExpression;
            return next != null ?
                $"{CompileMemberName(next)}.{expr.Member.Name}" :
                expr.Member.Name;
        }
    }

    class Mapped
    {
        public readonly string From;
        public readonly string To;

        public Mapped(string from, string to)
        {
            From = from;
            To = to;
        }
    }
}
