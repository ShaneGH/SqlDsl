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

        static SqlStatementBuilder ToSqlBuilder(IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables, ISqlBuilder wrappedBuilder, ISqlStatement wrappedStatement, BuildMapState state)
        {
            var rowIdPropertyMap = tables
                .Select(t => (rowIdColumnName: $"{t.From}.{SqlStatementConstants.RowIdName}", resultClassProperty: t.To))
                .Enumerate();

            var mappedValues = properties
                .Select(x => (
                    type: x.MappedPropertyType, 
                    from: AddRoot(x.FromParamRoot, x.From, state), 
                    to: x.To, 
                    propertySegmentConstructors: x.PropertySegmentConstructors))
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
                    argConstructors: col.propertySegmentConstructors);
            }

            foreach (var col in rowIdPropertyMap)
                builder.RowIdsForMappedProperties.Add((col.rowIdColumnName, col.resultClassProperty));
                
            return builder;
        }

        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        public ICompiledQuery<TArgs, TMapped> Compile(ILogger logger = null)
        {
            var timer = new Timer(true);
            var result = Compile(Query, Mapper, logger: logger);

            if (logger.CanLogInfo(LogMessages.CompiledQuery))
                logger.LogInfo($"Query compiled in {timer.SplitString()}", LogMessages.CompiledQuery);

            return result;
        }

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

            if (query.PrimaryTableMember == null)
            {
                throw new InvalidOperationException("The query must have at least one select table.");
            }

            var state = new BuildMapState(query.PrimaryTableMember.Value.name, mutableParameters, mapper.Parameters[0], wrappedStatement);
            var (resultType, properties, tables) = BuildMapFromRoot(state, mapper.Body);

            switch (resultType)
            {
                case BuildMapResult.Map:
                    return ToSqlBuilder(properties, tables, wrappedBuilder, wrappedStatement, state)
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

        public Task<List<TMapped>> ToListAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).ToListAsync(executor, args, logger: logger);

        public List<TMapped> ToList(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).ToList(executor, args, logger: logger);

        public Task<TMapped[]> ToArrayAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).ToArrayAsync(executor, args, logger: logger);

        public TMapped[] ToArray(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger: logger).ToArray(executor, args, logger: logger);

        static readonly IEnumerable<MappedProperty> EmptyMapped = Enumerable.Empty<MappedProperty>();

        static (BuildMapResult resultType, IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapFromRoot(BuildMapState state, Expression expr)
        {
            var _expr = ReflectionUtils.RemoveConvert(expr);

            var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(_expr, allowOne: true, allowSelect: true);
            if (isPropertyChain)
            {
                if (root != state.QueryObject)
                    throw new InvalidOperationException("Unable to understand mapping statement: " + expr);

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
                            new[]{ new MappedProperty(root, pChain, null, expr.Type) },
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
                            new[]{ new MappedProperty(root, pChain, null, GetSimplePropertyCellType(expr, state.QueryObject)) },
                            EmptyMapped
                        );
                    }
                }

                // TODO: there is a third case where mapping expr is x => x.Outer (and Outer is {Inner: {Val: string}}
                throw new InvalidOperationException("Unable to understand mapping statement: " + expr);
            }

            var (properties, tables) = BuildMap(state, expr, MapType.Root, isExprTip: true);

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
            MapType nextMap,
            string toPrefix = null, 
            bool isExprTip = false)
        {
            if (ReflectionUtils.IsConstant(expr))
            {
                var result = Expression
                    .Lambda<Func<object>>(
                        ReflectionUtils.Convert(
                            expr,
                            typeof(object)))
                    .Compile()();

                state.Parameters.Add(result);

                return (
                    new MappedProperty(null, "@p" + (state.Parameters.Count - 1), toPrefix, expr.Type).ToEnumerable(),
                    EmptyMapped
                );
            }

            if (isExprTip && ExprRepresentsTable(state, expr))
                expr = ReflectionUtils.ConvertToFullMemberInit(expr);

            switch (expr.NodeType)
            {
                case ExpressionType.Parameter:
                    return (
                        new [] { new MappedProperty(expr as ParameterExpression, null, toPrefix, expr.Type) },
                        EmptyMapped
                    );
                    
                case ExpressionType.MemberAccess:
                    return BuildMapForMemberAccess(state, expr as MemberExpression, toPrefix);

                case ExpressionType.Block:
                    return (expr as BlockExpression).Expressions
                        .Select(ex => BuildMap(state, ex, MapType.Other, toPrefix))
                        .AggregateTuple2();

                case ExpressionType.New:
                    return BuildMapForConstructor(state, expr as NewExpression, toPrefix: toPrefix);

                case ExpressionType.MemberInit:
                    return BuildMapForMemberInit(state, expr as MemberInitExpression, toPrefix);

                case ExpressionType.Call:
                    var oneExpr = ReflectionUtils.IsOne(expr as MethodCallExpression);
                    if (oneExpr != null)
                        // .One(...) is invisible as far as nextMap is concerned
                        return BuildMap(state, oneExpr, nextMap, toPrefix);
                        
                    var (isToList, enumerableL) = ReflectionUtils.IsToList(expr as MethodCallExpression);
                    if (isToList)
                        // .ToList(...) is invisible as far as nextMap is concerned
                        return BuildMap(state, enumerableL, nextMap, toPrefix);
                        
                    var (isToArray, enumerableA) = ReflectionUtils.IsToArray(expr as MethodCallExpression);
                    if (isToArray)
                        // .ToArray(...) is invisible as far as nextMap is concerned
                        return BuildMap(state, enumerableA, nextMap, toPrefix);

                    var (isSelect, enumerableS, mapper) = ReflectionUtils.IsSelectWithLambdaExpression(expr as MethodCallExpression);
                    if (isSelect)
                        return BuildMapForSelect(state, enumerableS, mapper, toPrefix, isExprTip);

                    var (isJoined, joinedFrom, joinedTo) = ReflectionUtils.IsJoined(expr as MethodCallExpression);
                    if (isJoined)
                    {
                        // the join statement needs to end in a Select(...) call
                        if (nextMap != MapType.Select && nextMap != MapType.MemberAccess)
                        {
                            expr = ReflectionUtils.ConvertToFullMemberInit(expr);
                            return BuildMap(state, expr, nextMap, toPrefix, isExprTip);
                        }

                        return BuildMapForJoined(state, joinedFrom, joinedTo, toPrefix);
                    }

                    break;

                default:
                    break;
            }

            throw new InvalidOperationException($"Unsupported mapping expression \"{expr}\".");
        }

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForConstructor(BuildMapState state, NewExpression expr, string toPrefix = null)
        { 
            return expr.Arguments
                .Select(ex => BuildMap(state, ex, MapType.Other, toPrefix: null, isExprTip: true))
                .Select((map, i) => (
                    map.properties.Select(p => new MappedProperty(
                        p.FromParamRoot,
                        p.From, 
                        CombineStrings(toPrefix, CombineStrings($"{SqlStatementConstants.ConstructorArgPrefixAlias}{i}", p.To)), 
                        p.MappedPropertyType,
                        constructorArgs: p.PropertySegmentConstructors.Prepend(expr.Constructor).ToArray())), 
                        // TODO: $"{SqlStatementConstants.ConstructorArgPrefixAlias}{i}" is repeated in code a lot
                    map.tables.Select(x => new MappedTable(x.From, CombineStrings($"{SqlStatementConstants.ConstructorArgPrefixAlias}{i}", x.To)))))
                .AggregateTuple2();
        }

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForMemberAccess(BuildMapState state, MemberExpression expr, string toPrefix = null)
        {
            var result = BuildMap(state, expr.Expression, MapType.MemberAccess, toPrefix);

            return (
                result.properties
                    .Select(p => new MappedProperty(
                        p.FromParamRoot,
                        CombineStrings(p.From, expr.Member.Name), 
                        p.To,
                        expr.Type))
                    .Enumerate(),
                result.tables
            );
        }

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForMemberInit(BuildMapState state, MemberInitExpression expr, string toPrefix = null)
        {
            return BuildMap(state, expr.NewExpression, MapType.Other, toPrefix)
                .ToEnumerableStruct()
                .Concat(expr.Bindings
                    .OfType<MemberAssignment>()
                    .Select(b => (binding: b, memberName: b.Member.Name, map: BuildMap(state, b.Expression, MapType.Other, b.Member.Name)))
                    .Select(m => (
                        m.map.properties.SelectMany(x => 
                        {
                            if (PropertyRepresentsTable(state, x))
                            {
                                var t = m.binding.Member
                                    .GetPropertyOrFieldType();

                                t = ReflectionUtils.GetIEnumerableType(t) ?? t;
                                return t
                                    .GetFieldsAndProperties()
                                    .Select(mem => new MappedProperty(x.FromParamRoot, CombineStrings(x.From, mem.name), CombineStrings(x.To, mem.name), mem.type));
                            }

                            return new MappedProperty(
                                x.FromParamRoot, 
                                x.From, 
                                CombineStrings(toPrefix, x.To),
                                x.MappedPropertyType, 
                                x.PropertySegmentConstructors).ToEnumerable();
                        }),
                        m.map.tables.Select(x => new MappedTable(x.From, CombineStrings(m.memberName, x.To))))))
                .AggregateTuple2();
        }

        static bool PropertyRepresentsTable(BuildMapState state, MappedProperty property)
        {
            // mapped property points to the root query object
            if (property.From == null && property.FromParamRoot == state.QueryObject)
                return true;

            // mapped property points to a table on the query object
            if (property.FromParamRoot == state.QueryObject && state.WrappedSqlStatement.ContainsTable(property.From))
                return true;

            // mapped property points to a table on the query object
            if (property.From == null && state.ParameterRepresentsProperty.Any(p => p.parameter == property.FromParamRoot))
                return true;

            return false;
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

            var outerMap = BuildMap(state, enumerable, MapType.Select, toPrefix);
            var innerMap = BuildMap(state, mapper.Body, MapType.Other, isExprTip: isExprTip);
            var outerMapProperties  = outerMap.properties.Enumerate();
            
            var (isSuccess, name) = CompileMemberName(enumerable);
            var newTableMap = isSuccess ?
                new MappedTable(name, null).ToEnumerable()
                : EmptyMapped;

            return (
                outerMapProperties
                    .SelectMany(r => innerMap.properties
                        .Select(m => m.FromParamRoot != null ?
                            new MappedProperty(m.FromParamRoot, m.From, CombineStrings(r.To, m.To), m.MappedPropertyType, m.PropertySegmentConstructors) :
                            new MappedProperty(r.FromParamRoot, CombineStrings(r.From, m.From), CombineStrings(r.To, m.To), m.MappedPropertyType, m.PropertySegmentConstructors))),
                outerMap.tables
                    .Concat(innerMap.tables)
                    .Concat(newTableMap)
            );
        }

        /// <summary>
        /// Verify whether a call to Sql.Joined is using parameters which represent an actual join
        /// </summary>
        /// <param name="originalFrom">The VerifyJoin method can modify expressions, which may lead to a confusing error message. Use this paramater if a different expression should appear in the error message</summary>
        static void VerifyJoin(BuildMapState state, Expression from, string to, Expression originalFrom = null)
        {
            switch (from.NodeType)
            {
                case ExpressionType.Call:
                    var (isJoined, joinedFrom, joinedTo) = ReflectionUtils.IsJoined(from as MethodCallExpression);
                    if (!isJoined)
                        throw new InvalidOperationException($"Property joined from is invalid\nfrom: {originalFrom ?? from}, to: {to}");

                    var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(joinedTo);
                    if (!isPropertyChain)
                        throw new InvalidOperationException($"Property joined to is invalid\nfrom: {originalFrom ?? from}, to: {to}");

                    VerifyJoin(state, joinedFrom, chain.JoinString("."));
                    break;
                case ExpressionType.Parameter:
                    // if from is the query object, convert it to the primary table
                    if (from == state.QueryObject)
                    {
                        VerifyJoin(state, state.PrimarySelectTable, to);
                        return;
                    }

                    string from_ = null;
                    from_ = state.ParameterRepresentsProperty
                        .Where(s => s.parameter == from)
                        .Select(s => s.property.JoinString("."))
                        .FirstOrDefault();

                    if (from_ == null)
                        throw new InvalidOperationException($"Property joined from is invalid\nfrom: {originalFrom ?? from}, to: {to}");

                    VerifyJoin(state, from_, to);

                    break;

                case ExpressionType.MemberAccess:
                    var member = from as MemberExpression;
                    if (member.Expression != state.QueryObject)
                        throw new InvalidOperationException($"Property joined from is invalid\nfrom: {originalFrom ?? from}, to: {to}");

                    VerifyJoin(state, member.Member.Name, to);

                    break;

                default:
                    throw new InvalidOperationException($"Property join is invalid\nfrom: {originalFrom ?? from}, to: {to}");
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
            var op = BuildMap(state, joinedTo, MapType.Other, toPrefix);

            var propsEnumerated = op.properties.ToList();
            if (propsEnumerated.Count != 1)
                throw new InvalidOperationException("A join must contain a reference to 1 table on the query object");
                
            VerifyJoin(state, joinedFrom, propsEnumerated[0].From);

            return (
                propsEnumerated.Select(BuildProp),
                op.tables);

            MappedProperty BuildProp(MappedProperty x) => new MappedProperty(
                state.QueryObject,
                x.From, 
                //$"{SqlStatementConstants.RootObjectAlias}.{x.From}", 
                x.To, 
                x.MappedPropertyType, 
                x.PropertySegmentConstructors);
        }

        //static string RootObjectAsPrefix => $"{SqlStatementConstants.RootObjectAlias}.";

        static string CombineStrings(string s1, string s2)
        {
            return s1 == null && s2 == null ?
                null :
                s1 != null && s2 != null ? 
                    $"{s1}.{s2}" :
                    $"{s1}{s2}";
        }

        static string AddRoot(ParameterExpression root, string property, BuildMapState state)
        {
            // I am not 100% sure about the "root == state.QueryObject" part
            if (root == null || root == state.QueryObject) return property;

            var propertyRoot = state.ParameterRepresentsProperty
                .Where(p => p.parameter == root)
                .Select(p => p.property.JoinString("."))
                .FirstOrDefault();

            if (propertyRoot == null)
                throw new InvalidOperationException($"Could not find alias for mapping parameter \"{root}\".");

            if (!string.IsNullOrEmpty(property))
                propertyRoot += ".";

            return $"{propertyRoot}{property}";
        }

        static (bool isSuccess, string name) CompileMemberName(Expression expr)
        {
            var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(expr, allow1Join: true);
            if (!isPropertyChain) return (false, null);

            return (true, chain.JoinString("."));
        }

        class BuildMapState
        {
            public readonly List<object> Parameters;
            public readonly ParameterExpression QueryObject;
            public readonly List<(ParameterExpression parameter, IEnumerable<string> property)> ParameterRepresentsProperty = new List<(ParameterExpression, IEnumerable<string>)>();
            public readonly ISqlStatement WrappedSqlStatement;
            public readonly string PrimarySelectTable;

            public BuildMapState(string primarySelectTable, List<object> parameters, ParameterExpression queryObject, ISqlStatement wrappedSqlStatement)
            {
                Parameters = parameters;
                QueryObject = queryObject;
                WrappedSqlStatement = wrappedSqlStatement;
                PrimarySelectTable = primarySelectTable;
            }
        }
        
        enum BuildMapResult
        {
            Map = 1,
            SimpleProp,
            SingleComplexProp,
            MultiComplexProp
        }

        enum MapType
        {
            Root = 1,
            Select,
            MemberAccess,
            Other
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
        static readonly ConstructorInfo[] EmptyConstructorArgs = new ConstructorInfo[0];

        public readonly Type MappedPropertyType;
        public readonly ConstructorInfo[] PropertySegmentConstructors;
        public readonly ParameterExpression FromParamRoot;

        public MappedProperty(ParameterExpression fromParamRoot, string from, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs = null)
            : base(from, to)
        {
            FromParamRoot = fromParamRoot;
            MappedPropertyType = mappedPropertyType;
            PropertySegmentConstructors = constructorArgs ?? EmptyConstructorArgs;
        }
    }
}
