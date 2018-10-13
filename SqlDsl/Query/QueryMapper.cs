using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.SqlBuilders;
using SqlDsl.SqlBuilders.SqlStatementParts;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Query
{
    public class QueryMapper<TSqlBuilder, TArgs, TResult, TMapped> : ISqlBuilder<TArgs, TMapped>
        where TSqlBuilder: ISqlFragmentBuilder, new()
    {
        readonly QueryBuilder<TSqlBuilder, TArgs, TResult> Query;
        readonly Expression<Func<TResult, TMapped>> Mapper;
        
        public QueryMapper(QueryBuilder<TSqlBuilder, TArgs, TResult> query, Expression<Func<TResult, TMapped>> mapper)
        {
            Query = query ?? throw new ArgumentNullException(nameof(query));
            Mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        }

        static SqlStatementBuilder<TSqlBuilder> ToSqlBuilder(IEnumerable<Mapped> properties, IEnumerable<Mapped> tables, ISqlBuilder wrappedBuilder, ISqlStatement wrappedStatement)
        {
            var rowIdPropertyMap = tables
                .Select(t => (rowIdColumnName: $"{t.From}.{SqlStatementConstants.RowIdName}", resultClassProperty: t.To))
                .Enumerate();

            var mappedValues = properties
                .Select(x => (from: RemoveRoot(x.From), to: RemoveRoot(x.To)))
                .Enumerate();

            var builder = new SqlStatementBuilder<TSqlBuilder>();
            builder.SetPrimaryTable(wrappedBuilder, wrappedStatement, wrappedStatement.UniqueAlias);

            foreach (var col in mappedValues)
                builder.AddSelectColumn(
                    col.from, 
                    tableName: (col.from ?? "").StartsWith("@") ? null : wrappedStatement.UniqueAlias, 
                    alias: col.to);

            foreach (var col in rowIdPropertyMap)
                builder.RowIdsForMappedProperties.Add((col.rowIdColumnName, col.resultClassProperty));
                
            return builder;
        }

        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        public ICompiledQuery<TArgs, TMapped> Compile()
        {
            // TODO: filter columns
            // var wrappedSql = Query.ToSqlBuilder(MappedValues.Select(m => m.from));

            var (wrappedBuilder, parameters) = Query.ToSqlStatement(null);
            var mutableParameters = parameters.ToList();
            var wrappedStatement = new SqlStatement(wrappedBuilder);

            var (resultType, properties, tables) = BuildMapFromRoot(
                new BuildMapState(mutableParameters, Mapper.Parameters[0], wrappedStatement),
                Mapper.Body);

            switch (resultType)
            {
                case BuildMapResult.Map:
            
                    return ToSqlBuilder(properties, tables, wrappedBuilder, wrappedStatement)
                        .Compile<TArgs, TMapped>(mutableParameters.Skip(0), QueryParseType.ORM);
                case BuildMapResult.SimpleProp:
                    properties = properties.Enumerate();
                    if (properties.Count() != 1)
                    {
                        throw new InvalidOperationException($"Expected one property, but got {properties.Count()}.");
                    }

                    return ToSqlBuilder(properties.First().From, wrappedBuilder, wrappedStatement)
                        .CompileSimple<TArgs, TMapped>(mutableParameters.Skip(0), properties.First().From);
                default:
                    // TODO: BuildMapResult.ComplexProp
                    throw new NotSupportedException(resultType.ToString());
            }
        }

        static SqlStatementBuilder<TSqlBuilder> ToSqlBuilder(string property, ISqlBuilder wrappedBuilder, ISqlStatement wrappedStatement)
        {
            var builder = new SqlStatementBuilder<TSqlBuilder>();
            builder.SetPrimaryTable(wrappedBuilder, wrappedStatement, wrappedStatement.UniqueAlias);
            builder.AddSelectColumn(
                property, 
                tableName: wrappedStatement.UniqueAlias);
                
            return builder;
        }
       
        public Task<IEnumerable<TMapped>> ExecuteAsync(IExecutor executor, TArgs args) =>
            Compile().ExecuteAsync(executor, args);
        
        public IEnumerable<TMapped> Execute(IExecutor executor, TArgs args) =>
            Compile().Execute(executor, args);

        static readonly IEnumerable<Mapped> EmptyMapped = Enumerable.Empty<Mapped>();

        static (BuildMapResult resultType, IEnumerable<Mapped> properties, IEnumerable<Mapped> tables) BuildMapFromRoot(BuildMapState state, Expression expr)
        {
            var _expr = ReflectionUtils.RemoveConvert(expr);
            _expr = ReflectionUtils.IsOne(_expr) ?? _expr;

            var (isPropertyChain, _, chain) = ReflectionUtils.GetPropertyChain(_expr);
            if (isPropertyChain)
            {
                var pChain = chain.JoinString(".");
                if (pChain == "")
                {
                    throw new InvalidOperationException("You must provide a valid mapping with the Map(...) method.");
                }

                foreach (var property in state.WrappedSqlStatement.Tables)
                {
                    if (pChain == property.Alias)
                        return (
                            BuildMapResult.ComplexProp,
                            new[]{ new Mapped(pChain, null) },
                            Enumerable.Empty<Mapped>()
                        );

                    if (pChain.StartsWith(property.Alias) && 
                        pChain.Length >= (property.Alias.Length + 2) &&
                        pChain[property.Alias.Length] == '.' &&
                        !pChain.Substring(property.Alias.Length + 1).Contains('.'))
                    {
                        return (
                            BuildMapResult.SimpleProp,
                            new[]{ new Mapped(pChain, null) },
                            Enumerable.Empty<Mapped>()
                        );
                    }
                }

                // TODO: there is a third case where mapping expr is x => x.Outer (and Outer is {Inner: {Val: string}}
                throw new InvalidOperationException("Unable to understand mapping statement: " + expr);
            }

            var (properties, tables) = BuildMap(state, expr);
            return (BuildMapResult.Map, properties, tables);
        }

        static bool IsConstant(Expression expr)
        {
            switch (expr.NodeType)
            {
                case ExpressionType.Constant:
                    return true;
                case ExpressionType.MemberAccess:
                    return IsConstant((expr as MemberExpression).Expression);
                case ExpressionType.Call:
                    var call = expr as MethodCallExpression;
                    return (call.Object == null || IsConstant(call.Object)) && call.Arguments.All(IsConstant);
                default:
                    return false;
            }
        }

        static (IEnumerable<Mapped> properties, IEnumerable<Mapped> tables) BuildMap(BuildMapState state, Expression expr, string toPrefix = null)
        {
            if (IsConstant(expr))
            {
                var result = Expression
                    .Lambda<Func<object>>(
                        Expression.Convert(
                            expr,
                            typeof(object)))
                    .Compile()();

                state.Parameters.Add(result);

                return (
                    new Mapped("@p" + (state.Parameters.Count - 1), toPrefix).ToEnumerable(),
                    Enumerable.Empty<Mapped>()
                );
            }

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
                    return BuildMapForMemberAccess(state, expr as MemberExpression, toPrefix);

                case ExpressionType.Block:
                    return (expr as BlockExpression).Expressions
                        .Select(ex => BuildMap(state, ex, toPrefix))
                        .AggregateTuple2();

                case ExpressionType.New:
                    return (expr as NewExpression).Arguments
                        .Select(ex => BuildMap(state, ex, toPrefix))
                        .AggregateTuple2();

                case ExpressionType.MemberInit:
                    return BuildMapForMemberInit(state, expr as MemberInitExpression, toPrefix);

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

                default:
                    break;
            }

            throw new InvalidOperationException($"Unsupported mapping expression \"{expr}\".");
        }

        static (IEnumerable<Mapped> properties, IEnumerable<Mapped> tables) BuildMapForMemberAccess(BuildMapState state, MemberExpression expr, string toPrefix = null)
        {
            var result = BuildMap(state, expr.Expression, toPrefix);

            return (
                result.properties
                    .Select(p => new Mapped(
                        CombineStrings(p.From, expr.Member.Name), 
                        p.To))
                    .Enumerate(),
                result.tables
            );
        }

        static (IEnumerable<Mapped> properties, IEnumerable<Mapped> tables) BuildMapForMemberInit(BuildMapState state, MemberInitExpression expr, string toPrefix = null)
        {
            return BuildMap(state, expr.NewExpression, toPrefix)
                .ToEnumerableStruct()
                .Concat(expr.Bindings
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
        }

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
            public readonly List<object> Parameters;
            public readonly ParameterExpression QueryObject;
            public readonly List<(ParameterExpression parameter, IEnumerable<string> property)> ParameterRepresentsProperty = new List<(ParameterExpression, IEnumerable<string>)>();
            public readonly ISqlStatement WrappedSqlStatement;

            public BuildMapState(List<object> parameters, ParameterExpression queryObject, ISqlStatement wrappedSqlStatement)
            {
                Parameters = parameters;
                QueryObject = queryObject;
                WrappedSqlStatement = wrappedSqlStatement;
            }
        }
        
        enum BuildMapResult
        {
            Map = 1,
            SimpleProp,
            ComplexProp
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
