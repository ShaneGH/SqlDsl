using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.SqlBuilders;
using SqlDsl.SqlBuilders.SqlStatementParts;
using SqlDsl.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace SqlDsl.Query
{
    public class QueryMapper<TSqlBuilder, TArgs, TResult, TMapped> : ISqlBuilder<TArgs, TMapped>
        where TSqlBuilder: ISqlFragmentBuilder, new()
    {
        readonly QueryBuilder<TSqlBuilder, TArgs, TResult> Query;
        readonly Expression<Func<TResult, TArgs, TMapped>> Mapper;
        readonly ISqlFragmentBuilder SqlFragmentBuilder = new TSqlBuilder();
        
        public QueryMapper(QueryBuilder<TSqlBuilder, TArgs, TResult> query, Expression<Func<TResult, TArgs, TMapped>> mapper)
        {
            Query = query ?? throw new ArgumentNullException(nameof(query));
            Mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            SqlFragmentBuilder = query.SqlFragmentBuilder;
        }

        static SqlStatementBuilder ToSqlBuilder(ISqlFragmentBuilder sqlFragmentBuilder, IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables, ISqlBuilder wrappedBuilder, ISqlStatement wrappedStatement, BuildMapState state)
        {
            var rowIdPropertyMap = tables
                .Select(t => (rowIdColumnName: $"{t.From}.{SqlStatementConstants.RowIdName}", resultClassProperty: t.To))
                .Enumerate();

            var mappedValues = properties
                .Select(x => (
                    type: x.MappedPropertyType, 
                    from: x.FromParams.BuildFromString(state, sqlFragmentBuilder, wrappedStatement.UniqueAlias),//  AddRoot(x.FromParamRoot, x.From, state), 
                    fromParams: x.FromParams.GetEnumerable1().Select(Accumulator.AddRoot(state)),
                    to: x.To, 
                    propertySegmentConstructors: x.PropertySegmentConstructors));

            var builder = new SqlStatementBuilder(sqlFragmentBuilder);
            builder.SetPrimaryTable(wrappedBuilder, wrappedStatement, wrappedStatement.UniqueAlias);

            foreach (var col in mappedValues)
            {
                // note: if AddSelectColumn is throwing an argument null exception
                // on alias, it probably means that a different ToSqlBuilder should be called
                
                var table = (col.from ?? "").StartsWith("@") ? null : wrappedStatement.UniqueAlias;
                builder.AddSelectColumn(
                    col.type,
                    col.from,
                    col.to,
                    col.fromParams.Select(p =>
                    {
                        return (
                            (p ?? "").StartsWith("@") ? null : wrappedStatement.UniqueAlias, 
                            p);
                    })
                    .ToArray(),
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
            var result = Compile(SqlFragmentBuilder, Query, Mapper, logger: logger);

            if (logger.CanLogInfo(LogMessages.CompiledQuery))
                logger.LogInfo($"Query compiled in {timer.SplitString()}", LogMessages.CompiledQuery);

            return result;
        }

        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        static ICompiledQuery<TArgs, TMapped> Compile(
            ISqlFragmentBuilder sqlFragmentBuilder, 
            QueryBuilder<TSqlBuilder, TArgs, TResult> query, 
            LambdaExpression mapper, 
            ILogger logger = null)
        {
            // TODO: filter columns
            // var wrappedSql = Query.ToSqlBuilder(MappedValues.Select(m => m.from));

            var (wrappedBuilder, parameters) = query.ToSqlStatement(null);
            var mutableParameters = new ParamBuilder(parameters.ToList());
            var wrappedStatement = new SqlStatement(wrappedBuilder);

            if (query.PrimaryTableMember == null)
            {
                throw new InvalidOperationException("The query must have at least one select table.");
            }

            var argsParam = mapper.Parameters.Count > 1 ? mapper.Parameters[1] : null;
            var state = new BuildMapState(query.PrimaryTableMember.Value.name, mutableParameters, mapper.Parameters[0], argsParam, wrappedStatement);
            var (resultType, properties, tables) = BuildMapFromRoot(state, mapper.Body);

            switch (resultType)
            {
                case BuildMapResult.Map:
                    return ToSqlBuilder(sqlFragmentBuilder, properties, tables, wrappedBuilder, wrappedStatement, state)
                        .Compile<TArgs, TMapped>(mutableParameters.Parameters, QueryParseType.ORM);

                case BuildMapResult.SimpleProp:
                    properties = properties.Enumerate();
                    if (properties.Count() != 1)
                    {
                        throw new InvalidOperationException($"Expected one property, but got {properties.Count()}.");
                    }

                    var p = properties.First();
                    return ToSqlBuilder(sqlFragmentBuilder, p.FromParams, p.MappedPropertyType, wrappedBuilder, wrappedStatement)
                        .CompileSimple<TArgs, TMapped>(mutableParameters.Parameters, SqlStatementConstants.SingleColumnAlias);

                case BuildMapResult.SingleComplexProp:
                    var init = Expression.Lambda<Func<TResult, TArgs, TMapped>>(
                        ReflectionUtils.ConvertToFullMemberInit(mapper.Body), 
                        mapper.Parameters);

                    return Compile(sqlFragmentBuilder, query, init, logger: logger);

                case BuildMapResult.MultiComplexProp:

                    // convert xs => xs to xs => xs.Select(x => new X { x1 = x.x1, x2 = x.x2 })
                    // this is easier for mapper to understand

                    var identityMap = Expression.Lambda(
                        AddMemberInitSelector(typeof(TMapped), mapper.Body), 
                        mapper.Parameters);

                    return Compile(sqlFragmentBuilder, query, identityMap, logger: logger);

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

        static SqlStatementBuilder<TSqlBuilder> ToSqlBuilder(ISqlFragmentBuilder sqlFragmentBuilder, Accumulator property, Type cellDataType, ISqlBuilder wrappedBuilder, ISqlStatement wrappedStatement)
        {
            var builder = new SqlStatementBuilder<TSqlBuilder>();
            builder.SetPrimaryTable(wrappedBuilder, wrappedStatement, wrappedStatement.UniqueAlias);

            var referencedColumns = new List<(string, string)>();
            string sql = null;
            string Add(string sqlPart, ExpressionType combiner)
            {
                if (!sqlPart.StartsWith("@"))
                {
                    referencedColumns.Add((wrappedStatement.UniqueAlias, sqlPart));
                    sqlPart = sqlFragmentBuilder.BuildSelectColumn(
                        wrappedStatement.UniqueAlias,
                        sqlPart);
                }

                return sql == null ?
                    sqlPart :
                    sqlFragmentBuilder.Concat(sql, sqlPart, combiner);
            }

            // second arg does not matter, as sql is null
            sql = Add(property.First.param, ExpressionType.ModuloAssign);
            foreach (var part in property.Next)
            {
                sql = Add(part.element.param, part.combiner);
            }

            builder.AddSelectColumn(
                cellDataType,
                sql,
                SqlStatementConstants.SingleColumnAlias,
                referencedColumns.ToArray());
                
            return builder;
        }

        static SqlStatementBuilder<TSqlBuilder> ToSqlBuilderXX(ISqlFragmentBuilder sqlFragmentBuilder, Accumulator property, Type cellDataType, ISqlBuilder wrappedBuilder, ISqlStatement wrappedStatement)
        {
            var builder = new SqlStatementBuilder<TSqlBuilder>();
            builder.SetPrimaryTable(wrappedBuilder, wrappedStatement, wrappedStatement.UniqueAlias);

            var propertyName = property.First.param;

            var col = propertyName.StartsWith("@") ? 
                propertyName :
                sqlFragmentBuilder.BuildSelectColumn(
                    wrappedStatement.UniqueAlias,
                    propertyName);

            builder.AddSelectColumn(
                cellDataType,
                col,
                propertyName,
                new [] { (wrappedStatement.UniqueAlias, propertyName) });
                
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

        static readonly IEnumerable<MappedTable> EmptyMappedTables = Enumerable.Empty<MappedTable>();

        static (BuildMapResult resultType, IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapFromRoot(BuildMapState state, Expression expression)
        {
            var (isPropertyChain, chains) = ReflectionUtils.GetPropertyChains(expression, allowOne: true, allowSelect: true, allowConstants: true);
            if (isPropertyChain)
            {
                var mappedChains = chains
                    .GetEnumerable2()
                    .Select(c => 
                    {
                        var ((root, chain, expr), combiner) = c;

                        if (root is ConstantExpression)
                        {                            
                            var chainExpr = chain
                                .Aggregate(root, Expression.PropertyOrField);

                            var value = Expression
                                .Lambda<Func<object>>(
                                    Expression.Convert(
                                        chainExpr,
                                        typeof(object)))
                                .Compile()();

                            var paramName = state.Parameters.AddParam(value);
                            return (
                                combiner: combiner,
                                type: BuildMapResult.SimpleProp,
                                result: new MappedProperty(null, paramName, null, chainExpr.Type)
                            );
                        }

                        var rootParam = root as ParameterExpression;
                        if (rootParam == null)
                            throw new InvalidOperationException($"Invalid root param type: {root}.");

                        if (rootParam == state.ArgsObject)
                        {
                            var result = QueryArgAccessor.Create(state.ArgsObject, expr);
                            var paramName = state.Parameters.AddParam(result);

                            return (
                                combiner: combiner,
                                type: BuildMapResult.SimpleProp,
                                result: new MappedProperty(null, paramName, null, expr.Type)
                            );
                        }

                        if (rootParam != state.QueryObject)
                            throw new InvalidOperationException("Unable to understand mapping statement: " + expr);

                        var pChain = chain.JoinString(".");
                        if (pChain == "")
                            throw new InvalidOperationException("You must provide a valid mapping with the Map(...) method.");

                        foreach (var property in state.WrappedSqlStatement.Tables)
                        {
                            // get property from one table (no query object)
                            if (property.Alias == SqlStatementConstants.RootObjectAlias && !pChain.Contains("."))
                            {
                                return (
                                    combiner: combiner,
                                    type: BuildMapResult.SimpleProp,
                                    result: new MappedProperty(rootParam, pChain, null, GetSimplePropertyCellType(expr, state.QueryObject, 1))
                                );
                            }

                            if (pChain == property.Alias)
                            {
                                // cannot support (1 + new { ... })
                                if (chains.Next.Any())
                                    throw new InvalidOperationException("Unable to understand mapping statement: " + expr);

                                var resultType = ReflectionUtils.GetIEnumerableType(expr.Type) == null ?
                                    BuildMapResult.SingleComplexProp :
                                    BuildMapResult.MultiComplexProp;
                                    
                                return (
                                    combiner: combiner,
                                    type: resultType,
                                    result: new MappedProperty(rootParam, pChain, null, expr.Type)
                                );
                            }

                            if (pChain.StartsWith(property.Alias) && 
                                pChain.Length >= (property.Alias.Length + 2) &&
                                pChain[property.Alias.Length] == '.' &&
                                !pChain.Substring(property.Alias.Length + 1).Contains('.'))
                            {
                                return (
                                    combiner: combiner,
                                    type: BuildMapResult.SimpleProp,
                                    result: new MappedProperty(rootParam, pChain, null, GetSimplePropertyCellType(expr, state.QueryObject, 2))
                                );
                            }
                        }

                        // TODO: there is a third case where mapping expr is x => x.Outer (and Outer is {Inner: {Val: string}}
                        throw new InvalidOperationException("Unable to understand mapping statement: " + expr);
                    })
                    .ToArray();

                if (mappedChains.Length == 0)
                    throw new InvalidOperationException("Unable to understand mapping statement: " + expression);

                if (mappedChains.Length == 1)
                {
                    return (
                        mappedChains[0].type,
                        mappedChains[0].result.ToEnumerable(),
                        EmptyMappedTables);
                }

                if (mappedChains.Any(x => x.type != BuildMapResult.SimpleProp))
                    throw new InvalidOperationException("Unable to understand mapping statement: " + expression);

                var combinedChains = mappedChains
                    .Skip(1)
                    .Select(c => (result: c.result.FromParams, combiner: c.combiner))
                    .Aggregate(
                        mappedChains[0].result.FromParams,
                        (x, y) => x.Combine(y.result, y.combiner ?? throw new InvalidOperationException("Expecting combination logic.")));

                return (
                    BuildMapResult.SimpleProp,
                    new [] 
                    { 
                        new MappedProperty(
                            new Accumulator(combinedChains),
                            null,
                            expression.Type) 
                    },
                    EmptyMappedTables
                );
            }

            var (properties, tables) = BuildMap(state, expression, MapType.Root, isExprTip: true);

            // if expression returns an object (e.g. a => a) the To param might be null
            tables = tables.Where(t => t.To != null);
            return (BuildMapResult.Map, properties, tables);
        }

        static Type GetSimplePropertyCellType(Expression simpleProperty, ParameterExpression queryRoot, int expectedChainLength)
        {
            var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(simpleProperty, allowOne: true, allowSelect: true);
            if (!isPropertyChain)
            {
                throw new InvalidOperationException($"Cannot find data cell type for expression {simpleProperty}");
            }

            chain = chain.Enumerate();
            if (chain.Count() != expectedChainLength)
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

                var paramName = state.Parameters.AddParam(result);

                return (
                    new MappedProperty(null, paramName, toPrefix, expr.Type).ToEnumerable(),
                    EmptyMappedTables
                );
            }

            if (isExprTip && ExprRepresentsTable(state, expr))
                expr = ReflectionUtils.ConvertToFullMemberInit(expr);

            switch (expr.NodeType)
            {
                case ExpressionType.Parameter:
                    return (
                        new [] { new MappedProperty(expr as ParameterExpression, null, toPrefix, expr.Type) },
                        EmptyMappedTables
                    );
                    
                case ExpressionType.Add:
                    return BuildMapForAdd(state, expr as BinaryExpression, toPrefix);
                    
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

                    break;

                default:
                    break;
            }

            throw new InvalidOperationException($"Unsupported mapping expression \"{expr}\".");
        }

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForConstructor(BuildMapState state, NewExpression expr, string toPrefix = null)
        { 
            return expr.Arguments
                .Select(ex => (type: ex.Type, map: BuildMap(state, ex, MapType.Other, toPrefix: null, isExprTip: true)))
                .Select((map, i) => (
                    map.Item2.properties.SelectMany(p => CreateContructorArg(p, map.type, i)), 
                    // TODO: $"{SqlStatementConstants.ConstructorArgPrefixAlias}{i}" is repeated in code a lot
                    map.map.tables.Select(x => new MappedTable(x.From, CombineStrings($"{SqlStatementConstants.ConstructorArgPrefixAlias}{i}", x.To)))))
                .AggregateTuple2();

            IEnumerable<MappedProperty> CreateContructorArg(MappedProperty arg, Type argType, int argIndex)
            {
                var many = PropertyRepresentsTable(state, arg) ?
                    SplitMapOfComplexProperty(arg, argType) :
                    arg.ToEnumerable();

                return many.Select(q => new MappedProperty(
                    q.FromParams,
                    CombineStrings(toPrefix, CombineStrings($"{SqlStatementConstants.ConstructorArgPrefixAlias}{argIndex}", q.To)), 
                    q.MappedPropertyType,
                    constructorArgs: q.PropertySegmentConstructors.Prepend(expr.Constructor).ToArray()));
            }
        }

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForAdd(BuildMapState state, BinaryExpression expr, string toPrefix = null)
        {
            var l = BuildMap(state, expr.Left, MapType.Other, toPrefix);
            var r = BuildMap(state, expr.Right, MapType.Other, toPrefix);

            var lProp = l.properties.ToArray();
            var rProp = r.properties.ToArray();

            if (lProp.Length != 1 && rProp.Length != 1)
                throw new InvalidOperationException($"Unsupported mapping expression \"{expr}\".");

            return (
                new MappedProperty(
                    lProp[0].FromParams.Combine(rProp[0].FromParams, ExpressionType.Add),
                    null, 
                    expr.Type).ToEnumerable(),
                l.tables.Concat(r.tables)
            );
        }

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForMemberAccess(BuildMapState state, MemberExpression expr, string toPrefix = null)
        {
            var result = BuildMap(state, expr.Expression, MapType.MemberAccess, toPrefix);
            var properties = result.properties.Enumerate();

            // cannot support (x + y).Value
            if (properties.Any(p => p.FromParams.Next.Any()))
                throw new InvalidOperationException("Unable to understand mapping statement: " + expr);

            return (
                result.properties
                    .Select(p => new MappedProperty(
                        p.FromParams.MapParamName(x => CombineStrings(x, expr.Member.Name)),
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
                                return SplitMapOfComplexProperty(x, m.binding.Member.GetPropertyOrFieldType());
                            }

                            return new MappedProperty(
                                x.FromParams, 
                                CombineStrings(toPrefix, x.To),
                                x.MappedPropertyType, 
                                x.PropertySegmentConstructors).ToEnumerable();
                        }),
                        m.map.tables.Select(x => new MappedTable(x.From, CombineStrings(m.memberName, x.To))))))
                .AggregateTuple2();
        }

        static IEnumerable<MappedProperty> SplitMapOfComplexProperty(MappedProperty property, Type propertyType)
        {
            propertyType = ReflectionUtils.GetIEnumerableType(propertyType) ?? propertyType;
            return propertyType
                .GetFieldsAndProperties()
                .Select(mem => new MappedProperty(
                    property.FromParams.MapParamName(x => CombineStrings(x, mem.name)), 
                    CombineStrings(property.To, mem.name), 
                    mem.type));
        }

        static bool PropertyRepresentsTable(BuildMapState state, MappedProperty property)
        {
            // a 2 part property (e.g. x + 1) cannot represent a table
            if (property.FromParams.Next.Any())
                return false;

            // mapped property points to the root query object
            if (property.FromParams.First.param == null && property.FromParams.First.paramRoot == state.QueryObject)
                return true;

            // mapped property points to a table on the query object
            if (property.FromParams.First.paramRoot == state.QueryObject && state.WrappedSqlStatement.ContainsTable(property.FromParams.First.param))
                return true;

            // mapped property points to a table on the query object
            if (property.FromParams.First.param == null && state.ParameterRepresentsProperty.Any(p => p.parameter == property.FromParams.First.paramRoot))
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
            var outerMapProperties  = outerMap.properties.ToArray();
            
            var (isSuccess, name) = CompileMemberName(enumerable);
            var newTableMap = isSuccess ?
                new MappedTable(name, null).ToEnumerable()
                : EmptyMappedTables;

            if (outerMapProperties.Length != 1 || outerMapProperties[0].FromParams.Next.Any())
                throw new InvalidOperationException($"Mapping from \"{enumerable}\" is not supported.");

            return (
                innerMap.properties
                    .Select(m => new MappedProperty(
                        m.FromParams.MapParam(x => (
                            x.paramRoot ?? outerMapProperties[0].FromParams.First.paramRoot, 
                            x.paramRoot == null ? CombineStrings(outerMapProperties[0].FromParams.First.param, x.param) : x.param)),
                        CombineStrings(outerMapProperties[0].To, m.To), 
                        m.MappedPropertyType, 
                        m.PropertySegmentConstructors)),
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

        static string CombineStrings(string s1, string s2)
        {
            return s1 == null && s2 == null ?
                null :
                s1 != null && s2 != null ? 
                    $"{s1}.{s2}" :
                    $"{s1}{s2}";
        }

        static (bool isSuccess, string name) CompileMemberName(Expression expr)
        {
            var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(expr);
            if (!isPropertyChain) return (false, null);

            return (true, chain.JoinString("."));
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

    class BuildMapState
    {
        public readonly ParamBuilder Parameters;
        public readonly ParameterExpression QueryObject;
        public readonly ParameterExpression ArgsObject;
        public readonly List<(ParameterExpression parameter, IEnumerable<string> property)> ParameterRepresentsProperty = new List<(ParameterExpression, IEnumerable<string>)>();
        public readonly ISqlStatement WrappedSqlStatement;
        public readonly string PrimarySelectTable;

        public BuildMapState(string primarySelectTable, ParamBuilder parameters, ParameterExpression queryObject, ParameterExpression argsObject, ISqlStatement wrappedSqlStatement)
        {
            Parameters = parameters;
            QueryObject = queryObject;
            ArgsObject = argsObject;
            WrappedSqlStatement = wrappedSqlStatement;
            PrimarySelectTable = primarySelectTable;
        }
    }

    //TODO: is paramRoot needed?
    class Accumulator : Accumulator<(ParameterExpression paramRoot, string param), ExpressionType>
    {   
        public Accumulator(
            ParameterExpression firstParamRoot, string firstParam, 
            IEnumerable<((ParameterExpression paramRoot, string param), ExpressionType)> next = null)
            : base((firstParamRoot, firstParam), next)
        {
        }
        
        public Accumulator(Accumulator<(ParameterExpression paramRoot, string param), ExpressionType> acc)
            : this(acc.First.paramRoot, acc.First.param, acc.Next)
        {
        }

        public Accumulator MapParam(Func<(ParameterExpression paramRoot, string param), (ParameterExpression, string)> map)
        {
            return new Accumulator(base.Map(map));
        }

        public Accumulator MapParamName(Func<string, string> map)
        {
            return MapParam(_Map);
            (ParameterExpression, string) _Map((ParameterExpression x, string y) z) => (z.x, map(z.y));
        }

        public Accumulator Combine(Accumulator x, ExpressionType combiner)
        {
            return new Accumulator(base.Combine(x, combiner));
        }

        public string BuildFromString(BuildMapState state, ISqlFragmentBuilder sqlFragmentBuilder, string wrappedQueryAlias)
        {
            var table1 = (First.param ?? "").StartsWith("@") ? null : wrappedQueryAlias;

            return Next.Aggregate(
                BuildColumn(table1, First.paramRoot, First.param),
                Aggregate);

            string Aggregate(string x, ((ParameterExpression paramRoot, string param) param, ExpressionType type) y)
            {
                var table = (y.param.param ?? "").StartsWith("@") ? null : sqlFragmentBuilder.WrapTable(wrappedQueryAlias);
                var yValue = BuildColumn(table, y.param.paramRoot, y.param.param);

                switch (y.type)
                {
                    case ExpressionType.Add:
                        return sqlFragmentBuilder.BuildAddCondition(x, yValue);

                    default:
                        throw new InvalidOperationException($"Cannot build accumulator for expression type {y.type}.");
                }
            }

            string BuildColumn(string tab, ParameterExpression paramRoot, string parameter)
            {
                return sqlFragmentBuilder.BuildSelectColumn(tab, AddRoot(paramRoot, parameter, state));
            }
        }

        public static Func<(ParameterExpression, string), string> AddRoot(BuildMapState state)
        {
            return Execute;

            string Execute((ParameterExpression root, string property) x)
            {
                var (root, property) = x;

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
        }

        public static string AddRoot(ParameterExpression root, string property, BuildMapState state)
        {
            return AddRoot(state)((root, property));
        }
    }

    class MappedProperty
    {
        static readonly ConstructorInfo[] EmptyConstructorArgs = new ConstructorInfo[0];

        public readonly Type MappedPropertyType;
        public readonly ConstructorInfo[] PropertySegmentConstructors;
        public readonly string To;
        public readonly Accumulator FromParams;

        public MappedProperty(Accumulator fromParams, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs = null)
        {
            To = to;
            FromParams = fromParams;
            MappedPropertyType = mappedPropertyType;
            PropertySegmentConstructors = constructorArgs ?? EmptyConstructorArgs;
        }
        
        public MappedProperty(ParameterExpression fromParamRoot, string from, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs = null)
            : this (new Accumulator(fromParamRoot, from), to, mappedPropertyType, constructorArgs)
        {
        }
    }
}
