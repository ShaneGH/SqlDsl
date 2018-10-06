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

        public (ISqlStatement builder, IEnumerable<object> paramaters) ToSqlBuilder()
        {
            // TODO: filter columns
            // var wrappedSql = Query.ToSqlBuilder(MappedValues.Select(m => m.from));

            var (wrappedBuilder, parameters) = Query.ToSqlBuilder(null);

            var map = BuildMap(
                new BuildMapState(Mapper.Parameters[0], wrappedBuilder),
                Mapper.Body);

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
                
            return (builder, parameters);
        }

        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        public ICompiledQuery<TMapped> Compile()
        {
            var sqlBuilder = ToSqlBuilder();
            return sqlBuilder.builder
                .Compile<TMapped>(sqlBuilder.paramaters, QueryParseType.ORM);
        }
        
        public Task<IEnumerable<TMapped>> ExecuteAsync(IExecutor executor) =>
            Compile().ExecuteAsync(executor);
        
        public IEnumerable<TMapped> Execute(IExecutor executor) =>
            Compile().Execute(executor);

        static readonly IEnumerable<Mapped> EmptyMapped = Enumerable.Empty<Mapped>();

        static (IEnumerable<Mapped> properties, IEnumerable<Mapped> tables) BuildMap(BuildMapState state, Expression expr, string toPrefix = null)
        {
            switch (expr.NodeType)
            {
                case ExpressionType.Parameter:
                    var hasProperty =state.ParameterRepresentsProperty
                        .Where(p => p.parameter == expr)
                        .Select(p => p.property.JoinString("."));

                    if (expr == state.QueryObject || hasProperty.Any())
                    {
                        return (
                            new [] { new Mapped(null, toPrefix) },
                            EmptyMapped
                        );
                    }

                    break;
                case ExpressionType.MemberAccess:
                    var memExpr = expr as MemberExpression;
                    var result = BuildMap(state, memExpr.Expression, toPrefix);

                    return (
                        result.properties
                            .Select(p => new Mapped(
                                CombineStrings(p.From, memExpr.Member.Name), 
                                p.To))
                            .Enumerate(),
                        result.tables
                    );

                case ExpressionType.Block:
                    return (expr as BlockExpression).Expressions
                        .Select(ex => BuildMap(state, ex, toPrefix))
                        .AggregateTuple2();

                case ExpressionType.New:
                    return (expr as NewExpression).Arguments
                        .Select(ex => BuildMap(state, ex, toPrefix))
                        .AggregateTuple2();

                case ExpressionType.MemberInit:
                    var init = expr as MemberInitExpression;
                    return BuildMap(state, init.NewExpression, toPrefix)
                        .ToEnumerableStruct()
                        .Concat(init.Bindings
                            .OfType<MemberAssignment>()
                            .Select(b => (binding: b, memberName: b.Member.Name, map: BuildMap(state, b.Expression, b.Member.Name)))
                            .Select(m => (
                                m.map.properties.SelectMany(x => 
                                {
                                    // if From == null, it is a reference to the query object
                                    if (x.From == null || state.WrappedSqlStatement.ContainsTable(RemoveRoot(x.From)))
                                    {
                                        var t = m.binding.Member
                                            .GetPropertyOrFieldType();

                                        t = ReflectionUtils.GetIEnumerableType(t) ?? t;
                                        return t
                                            .GetFieldsAndProperties()
                                            .Select(mem => new Mapped(CombineStrings(x.From, mem.name), CombineStrings(x.To, mem.name)));
                                    }

                                    return new Mapped(x.From, CombineStrings(toPrefix, x.To)).ToEnumerable();
                                }),
                                m.map.tables.Select(x => new Mapped(x.From, CombineStrings(m.memberName, x.To))))))
                        .AggregateTuple2();

                case ExpressionType.Call:
                    var oneExpr = ReflectionUtils.IsOne(expr as MethodCallExpression);
                    if (oneExpr != null)
                        return BuildMap(state, oneExpr, toPrefix);
                        
                    var (isToList, enumerableL) = ReflectionUtils.IsToList(expr as MethodCallExpression);
                    if (isToList)
                        return BuildMap(state, enumerableL, toPrefix);
                        
                    var (isToArray, enumerableA) = ReflectionUtils.IsToArray(expr as MethodCallExpression);
                    if (isToArray)
                        return BuildMap(state, enumerableA, toPrefix);

                    var (isSelect, enumerableS, mapper) = ReflectionUtils.IsSelectWithLambdaExpression(expr as MethodCallExpression);
                    if (isSelect)
                        return BuildMapForSelect(state, enumerableS, mapper, toPrefix);

                    var (isJoined, joinedFrom, joinedTo) = ReflectionUtils.IsJoined(expr as MethodCallExpression);
                    if (isJoined)
                        return BuildMapForJoined(state, joinedFrom, joinedTo, toPrefix);

                    break;
            }

            throw new InvalidOperationException($"Unsupported mapping expression \"{expr}\".");
        }

        // static bool IsQueryObjectProperty(BuildMapState state, string property)
        // {
        //     return state.AllPossibleTableProperties.Contains(property);
        // }

        static void TryAddSelectStatementParameterToProperty(BuildMapState state, Expression enumerable, ParameterExpression parameter)
        {
            var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(enumerable);
            if (isPropertyChain)
            {
                if (root == state.QueryObject)
                    state.ParameterRepresentsProperty.Add((parameter, chain));

                return;
            }

            if (enumerable is MethodCallExpression)
            {
                var (isJoined, joinedFrom, joinedTo) = ReflectionUtils.IsJoined(enumerable as MethodCallExpression);
                if (isJoined)
                    TryAddSelectStatementParameterToProperty(state, joinedTo, parameter);

                return;
            }
        }

        static (IEnumerable<Mapped> properties, IEnumerable<Mapped> tables) BuildMapForSelect(BuildMapState state, Expression enumerable, LambdaExpression mapper, string toPrefix)
        {
            TryAddSelectStatementParameterToProperty(state, enumerable, mapper.Parameters[0]);

            var outerMap = BuildMap(state, enumerable, toPrefix);
            var innerMap = BuildMap(state, mapper.Body);
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
                        throw new InvalidOperationException($"Property joined from is invalid\nfrom: {from}, to: {to}");
                        // TODO: better error message

                    var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(joinedTo);
                    if (!isPropertyChain)
                        throw new InvalidOperationException($"Property joined from is invalid\nfrom: {from}, to: {to}");
                        // TODO: better error message

                    VerifyJoin(state, joinedFrom, chain.JoinString("."));
                    break;
                case ExpressionType.Parameter:
                    var from_ = state.ParameterRepresentsProperty
                        .Where(s => s.parameter == from)
                        .Select(s => s.property.JoinString("."))
                        .FirstOrDefault();

                    if (from_ == null)
                        throw new InvalidOperationException($"Property joined from is invalid\nfrom: {from}, to: {to}");
                        // TODO: better error message

                    VerifyJoin(state, from_, to);

                    break;
                default:
                    throw new InvalidOperationException($"{from.GetType()}         Property joined from is invalid\nfrom: {from}, to: {to}");
                    // TODO: better error message
            }
        }

        /// <summary>
        /// Verify whether a join is valid. Throw an exception if not
        /// </summary>
        static void VerifyJoin(BuildMapState state, string from, string to)
        {
            if (!state.WrappedSqlStatement.JoinIsValid(from, to))
                throw new InvalidOperationException($"Error in mapping: property \"{from}\" does not join to property \"{to}\".");
        }

        static (IEnumerable<Mapped> properties, IEnumerable<Mapped> tables) BuildMapForJoined(BuildMapState state, Expression joinedFrom, Expression joinedTo, string toPrefix)
        {
            var op = BuildMap(state, joinedTo, toPrefix);

            var propsEnumerated = op.properties.ToList();
            if (propsEnumerated.Count != 1)
                throw new InvalidOperationException("A join must contain a reference to 1 table on the query object");
                
            VerifyJoin(state, joinedFrom, propsEnumerated[0].From);

            return (
                propsEnumerated
                    .Select(x => new Mapped($"{SqlStatementConstants.RootObjectAlias}.{x.From}", x.To)),
                op.tables);
        }

        static string RootObjectAsPrefix => $"{SqlStatementConstants.RootObjectAlias}.";

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

        static string CompileMemberName(MemberExpression expr)
        {
            var next = expr.Expression as MemberExpression;
            return next != null ?
                $"{CompileMemberName(next)}.{expr.Member.Name}" :
                expr.Member.Name;
        }

        class BuildMapState
        {
            public readonly ParameterExpression QueryObject;
            public readonly List<(ParameterExpression parameter, IEnumerable<string> property)> ParameterRepresentsProperty = new List<(ParameterExpression, IEnumerable<string>)>();
            public readonly ISqlStatement WrappedSqlStatement;
         //   public readonly IEnumerable<(string from, string to)> ValidJoins;
            //public readonly IEnumerable<string> AllPossibleTableProperties;

            public BuildMapState(ParameterExpression queryObject, ISqlStatement wrappedSqlStatement)//, string primaryTableProperty, IEnumerable<(string from, string to)> validJoins)
            {
                QueryObject = queryObject;
                WrappedSqlStatement = wrappedSqlStatement;
                // ValidJoins = validJoins.OrEmpty();
                
                // var tmp = ValidJoins.Select(x => x.from).Prepend(primaryTableProperty);
                // AllPossibleTableProperties = tmp.Concat(tmp.Select(x => CombineStrings(SqlStatementConstants.RootObjectAlias, x))).Enumerate();
            }
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
