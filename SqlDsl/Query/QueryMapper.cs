using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.SqlBuilders;
using SqlDsl.SqlBuilders.SqlStatementParts;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace SqlDsl.Query
{
    public class QueryMapper<TSqlBuilder, TArgs, TResult, TMapped> : ISqlBuilder<TArgs, TMapped>
        where TSqlBuilder: ISqlFragmentBuilder, new()
    {
        readonly QueryBuilder<TSqlBuilder, TArgs, TResult> Query;
        readonly Expression<Func<TResult, TArgs, TMapped>> Mapper;
        
        public QueryMapper(QueryBuilder<TSqlBuilder, TArgs, TResult> query, Expression<Func<TResult, TArgs, TMapped>> mapper)
        {
            Query = query ?? throw new ArgumentNullException(nameof(query));
            Mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        }

        static SqlStatementBuilder ToSqlBuilder(IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables, ISqlBuilder wrappedBuilder, ISqlStatement wrappedStatement)
        {
            var rowIdPropertyMap = tables
                .Select(t => (rowIdColumnName: $"{t.From}.{SqlStatementConstants.RowIdName}", resultClassProperty: t.To))
                .Enumerate();

            var mappedValues = properties
                .Select(x => (type: x.MappedPropertyType, from: RemoveRoot(x.From), to: RemoveRoot(x.To), isConstructorArg: x.IsConstructorArg))
                .Enumerate();

            var builder = new SqlStatementBuilder<TSqlBuilder>();
            builder.SetPrimaryTable(wrappedBuilder, wrappedStatement, wrappedStatement.UniqueAlias);

            foreach (var col in mappedValues)
            {
                builder.AddSelectColumn(
                    col.type,
                    col.from, 
                    tableName: (col.from ?? "").StartsWith("@") ? null : wrappedStatement.UniqueAlias, 
                    alias: col.to,
                    isForConstructor: col.isConstructorArg);
            }

            foreach (var col in rowIdPropertyMap)
                builder.RowIdsForMappedProperties.Add((col.rowIdColumnName, col.resultClassProperty));
                
            return builder;
        }

        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        public ICompiledQuery<TArgs, TMapped> Compile(ILogger logger = null) => Compile(Query, Mapper, logger: logger);

        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        static ICompiledQuery<TArgs, TMapped> Compile(
            QueryBuilder<TSqlBuilder, TArgs, TResult> query, 
            LambdaExpression mapper, 
            ILogger logger = null)
        {
            // TODO: filter columns
            // var wrappedSql = Query.ToSqlBuilder(MappedValues.Select(m => m.from));

            var (wrappedBuilder, parameters) = query.ToSqlStatement(null);
            var mutableParameters = parameters.ToList();
            var wrappedStatement = new SqlStatement(wrappedBuilder);

            var (resultType, properties, tables) = BuildMapFromRoot(
                new BuildMapState(mutableParameters, mapper.Parameters[0], wrappedStatement),
                mapper.Body);

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

                    var p = properties.First();
                    return ToSqlBuilder(p.From, p.MappedPropertyType, wrappedBuilder, wrappedStatement)
                        .CompileSimple<TArgs, TMapped>(mutableParameters.Skip(0), properties.First().From);

                case BuildMapResult.SingleComplexProp:
                    var init = Expression.Lambda<Func<TResult, TArgs, TMapped>>(
                        ReflectionUtils.ConvertToFullMemberInit(mapper.Body), 
                        mapper.Parameters);

                    return Compile(query, init, logger: logger);

                case BuildMapResult.MultiComplexProp:

                    // convert xs => xs to xs => xs.Select(x => new X { x1 = x.x1, x2 = x.x2 })
                    // this is easier for mapper to understand

                    var identityMap = Expression.Lambda(
                        AddMemberInitSelector(typeof(TMapped), mapper.Body), 
                        mapper.Parameters);

                    return Compile(query, identityMap, logger: logger);

                default:
                    throw new NotSupportedException(resultType.ToString());
            }
        }

        /// <summary>
        /// convert xs => xs to xs => xs.Select(x => new X { x1 = x.x1, x2 = x.x2 })
        /// </summary>
        static Expression AddMemberInitSelector(Type tMapped, Expression collection)
        {
            var enumeratedType = ReflectionUtils.GetIEnumerableType(tMapped);
            if (enumeratedType == null)
                throw new InvalidOperationException($"Expected type {tMapped} to implement IEnumerable<>");

            var innerParam = Expression.Parameter(enumeratedType);
            var mapperBody = ReflectionUtils.ConvertToFullMemberInit(innerParam);
            var mapper = Expression.Lambda(mapperBody, innerParam);

            return Expression.Call(
                ReflectionUtils
                    .GetMethod<IEnumerable<object>>(xs => xs.Select(x => x), enumeratedType, enumeratedType),
                collection,
                mapper);
        }

        static SqlStatementBuilder<TSqlBuilder> ToSqlBuilder(string propertyName, Type cellDataType, ISqlBuilder wrappedBuilder, ISqlStatement wrappedStatement)
        {
            var builder = new SqlStatementBuilder<TSqlBuilder>();
            builder.SetPrimaryTable(wrappedBuilder, wrappedStatement, wrappedStatement.UniqueAlias);
            builder.AddSelectColumn(
                cellDataType,
                propertyName, 
                tableName: wrappedStatement.UniqueAlias);
                
            return builder;
        }
       
        public Task<IEnumerable<TMapped>> ToIEnumerableAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).ToIEnumerableAsync(executor, args, logger: logger);
        
        public IEnumerable<TMapped> ToIEnumerable(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).ToIEnumerable(executor, args, logger: logger);

        static readonly IEnumerable<MappedProperty> EmptyMapped = Enumerable.Empty<MappedProperty>();

        static (BuildMapResult resultType, IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapFromRoot(BuildMapState state, Expression expr)
        {
            var _expr = ReflectionUtils.RemoveConvert(expr);

            var (isPropertyChain, _, chain) = ReflectionUtils.GetPropertyChain(_expr, allowOne: true, allowSelect: true);
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
                    {
                        var resultType = ReflectionUtils.GetIEnumerableType(_expr.Type) == null ?
                            BuildMapResult.SingleComplexProp :
                            BuildMapResult.MultiComplexProp;
                            
                        return (
                            resultType,
                            new[]{ new MappedProperty(pChain, null, expr.Type) },
                            EmptyMapped
                        );
                    }

                    if (pChain.StartsWith(property.Alias) && 
                        pChain.Length >= (property.Alias.Length + 2) &&
                        pChain[property.Alias.Length] == '.' &&
                        !pChain.Substring(property.Alias.Length + 1).Contains('.'))
                    {
                        return (
                            BuildMapResult.SimpleProp,
                            new[]{ new MappedProperty(pChain, null, GetSimplePropertyCellType(expr, state.QueryObject)) },
                            EmptyMapped
                        );
                    }
                }

                // TODO: there is a third case where mapping expr is x => x.Outer (and Outer is {Inner: {Val: string}}
                throw new InvalidOperationException("Unable to understand mapping statement: " + expr);
            }

            var (properties, tables) = BuildMap(state, expr, isExprTip: true);

            // if expression returns an object (e.g. a => a) the To param might be null
            tables = tables.Where(t => t.To != null);
            return (BuildMapResult.Map, properties, tables);
        }

        static Type GetSimplePropertyCellType(Expression simpleProperty, ParameterExpression queryRoot)
        {
            var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(simpleProperty, allowOne: true, allowSelect: true);
            if (!isPropertyChain)
            {
                throw new InvalidOperationException($"Cannot find data cell type for expression {simpleProperty}");
            }

            chain = chain.Enumerate();
            if (chain.Count() != 2)
            {
                throw new InvalidOperationException($"Cannot find data cell type for expression {simpleProperty}");
            }

            var (chainIsValid, type) = ReflectionUtils.GetTypeForPropertyChain(queryRoot.Type, chain);
            if (!chainIsValid)
            {
                throw new InvalidOperationException($"Cannot find data cell type for expression {simpleProperty}");
            }
            
            return type;
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

        static bool ExprRepresentsTable(BuildMapState state, Expression expr)
        {
            var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(expr);
            if (!isPropertyChain)
                return false;

            var chainPrefix = state.ParameterRepresentsProperty
                .Where(p => p.parameter == root)
                .Select(p => p.property)
                .FirstOrDefault();

            if (chainPrefix == null)
            {
                if (state.QueryObject != root)
                    return false;
                    
                chainPrefix = Enumerable.Empty<string>();
            }

            var property = chainPrefix.Concat(chain).JoinString(".");
            if (string.IsNullOrEmpty(property)) property = SqlStatementConstants.RootObjectAlias;
            return state.WrappedSqlStatement.ContainsTable(property);
        }

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMap(
            BuildMapState state, 
            Expression expr, 
            string toPrefix = null, 
            bool isExprTip = false)
        {
            if (IsConstant(expr))
            {
                var result = Expression
                    .Lambda<Func<object>>(
                        ReflectionUtils.Convert(
                            expr,
                            typeof(object)))
                    .Compile()();

                state.Parameters.Add(result);

                return (
                    new MappedProperty("@p" + (state.Parameters.Count - 1), toPrefix, expr.Type).ToEnumerable(),
                    EmptyMapped
                );
            }

            if (isExprTip && ExprRepresentsTable(state, expr))
                expr = ReflectionUtils.ConvertToFullMemberInit(expr);

            switch (expr.NodeType)
            {
                case ExpressionType.Parameter:
                    var hasProperty = state.ParameterRepresentsProperty
                        .Where(p => p.parameter == expr)
                        .Select(p => p.property.JoinString("."));

                    if (expr == state.QueryObject || hasProperty.Any())
                    {
                        return (
                            new [] { new MappedProperty(null, toPrefix, expr.Type) },
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
                    return BuildMapForConstructor(state, expr as NewExpression, toPrefix: toPrefix);

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
                        return BuildMapForSelect(state, enumerableS, mapper, toPrefix, isExprTip);

                    var (isJoined, joinedFrom, joinedTo) = ReflectionUtils.IsJoined(expr as MethodCallExpression);
                    if (isJoined)
                        return BuildMapForJoined(state, joinedFrom, joinedTo, toPrefix);

                    break;

                default:
                    break;
            }

            throw new InvalidOperationException($"Unsupported mapping expression \"{expr}\".");
        }

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForConstructor(BuildMapState state, NewExpression expr, string toPrefix = null)
        { 
            return expr.Arguments
                .Select(ex => BuildMap(state, ex, toPrefix: null, isExprTip: true))
                .Select((map, i) => (
                    map.properties.Select(p => new MappedProperty(
                        p.From, 
                        CombineStrings(toPrefix, CombineStrings($"{SqlStatementConstants.ConstructorArgPrefixAlias}{i}", p.To)), 
                        p.MappedPropertyType,
                        isConstructorArg: expr.Constructor)), 
                    map.tables))
                .AggregateTuple2();
        }

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForMemberAccess(BuildMapState state, MemberExpression expr, string toPrefix = null)
        {
            var result = BuildMap(state, expr.Expression, toPrefix);

            return (
                result.properties
                    .Select(p => new MappedProperty(
                        CombineStrings(p.From, expr.Member.Name), 
                        p.To,
                        expr.Type))
                    .Enumerate(),
                result.tables
            );
        }

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForMemberInit(BuildMapState state, MemberInitExpression expr, string toPrefix = null)
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
                                    .Select(mem => new MappedProperty(CombineStrings(x.From, mem.name), CombineStrings(x.To, mem.name), mem.type));
                            }

                            return new MappedProperty(x.From, CombineStrings(toPrefix, x.To), x.MappedPropertyType, x.IsConstructorArg).ToEnumerable();
                        }),
                        m.map.tables.Select(x => new MappedTable(x.From, CombineStrings(m.memberName, x.To))))))
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

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForSelect(BuildMapState state, Expression enumerable, LambdaExpression mapper, string toPrefix, bool isExprTip)
        {
            if (mapper.Body == mapper.Parameters[0])
            {
                throw new InvalidOperationException($"Mapping \"{mapper}\" is not supported.");
            }

            TryAddSelectStatementParameterToProperty(state, enumerable, mapper.Parameters[0]);

            var outerMap = BuildMap(state, enumerable, toPrefix);
            var innerMap = BuildMap(state, mapper.Body, isExprTip: isExprTip);
            var outerMapProperties  = outerMap.properties.Enumerate();
            
            var newTableMap = enumerable is MemberExpression ?
                new MappedTable(CompileMemberName(enumerable as MemberExpression), null).ToEnumerable() :
                EmptyMapped;

            return (
                outerMapProperties
                    .SelectMany(r => innerMap.properties
                        .Select(m => new MappedProperty(CombineStrings(r.From, m.From), CombineStrings(r.To, m.To), m.MappedPropertyType, m.IsConstructorArg))),
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
                    string from_ = null;
                    from_ = state.ParameterRepresentsProperty
                        .Where(s => s.parameter == from)
                        .Select(s => s.property.JoinString("."))
                        .FirstOrDefault();

                    if (from_ == null)
                        throw new InvalidOperationException($"Property joined from is invalid\nfrom: {from}, to: {to}");
                        // TODO: better error message

                    VerifyJoin(state, from_, to);

                    break;

                case ExpressionType.MemberAccess:
                    var member = from as MemberExpression;
                    if (member.Expression != state.QueryObject)
                        throw new InvalidOperationException($"Property joined from is invalid\nfrom: {from}, to: {to}");
                        // TODO: better error message

                    VerifyJoin(state, member.Member.Name, to);

                    break;

                default:
                    throw new InvalidOperationException($"Property joined from is invalid\nfrom: {from}, to: {to}");
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

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForJoined(BuildMapState state, Expression joinedFrom, Expression joinedTo, string toPrefix)
        {
            var op = BuildMap(state, joinedTo, toPrefix);

            var propsEnumerated = op.properties.ToList();
            if (propsEnumerated.Count != 1)
                throw new InvalidOperationException("A join must contain a reference to 1 table on the query object");
                
            VerifyJoin(state, joinedFrom, propsEnumerated[0].From);

            return (
                propsEnumerated
                    .Select(x => new MappedProperty($"{SqlStatementConstants.RootObjectAlias}.{x.From}", x.To, x.MappedPropertyType, x.IsConstructorArg)),
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
            SingleComplexProp,
            MultiComplexProp
        }
    }

    class MappedTable
    {
        public readonly string From;
        public readonly string To;

        public MappedTable(string from, string to)
        {
            From = from;
            To = to;
        }
    }

    class MappedProperty : MappedTable
    {
        public readonly Type MappedPropertyType;
        public readonly ConstructorInfo IsConstructorArg;

        public MappedProperty(string from, string to, Type mappedPropertyType, ConstructorInfo isConstructorArg = null)
            : base(from, to)
        {
            MappedPropertyType = mappedPropertyType;
            IsConstructorArg = isConstructorArg;
        }
    }
}
