using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    static class ComplexMapBuilder
    {
        static readonly IEnumerable<MappedTable> EmptyMappedTables = Enumerable.Empty<MappedTable>();

        public static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMap(BuildMapState state, Expression expr)
        {
            var (x, y, _) = BuildMap(state, expr, MapType.Root, isExprTip: true);
            return (x, y);
        }

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables, bool isConstant) BuildMap(
            BuildMapState state, 
            Expression expr, 
            MapType nextMap,
            string toPrefix = null, 
            bool isExprTip = false)
        {
            var (isConstant, requiresArgs) = ReflectionUtils.IsConstant(expr, state.ArgsObject);
            if (isConstant)
            {
                var result = requiresArgs ?
                    QueryArgAccessor.Create(state.ArgsObject, expr) :
                    Expression
                        .Lambda<Func<object>>(
                            ReflectionUtils.Convert(
                                expr,
                                typeof(object)))
                        .Compile()();

                var paramName = state.Parameters.AddParam(result);

                return (
                    new MappedProperty(null, paramName, toPrefix, expr.Type).ToEnumerable(),
                    EmptyMappedTables,
                    true
                );
            }

            if (isExprTip && ExprRepresentsTable(state, expr))
                expr = ReflectionUtils.ConvertToFullMemberInit(expr);

            switch (expr.NodeType)
            {
                case ExpressionType.Convert:
                    return BuildMap(state, (expr as UnaryExpression).Operand, nextMap, toPrefix, isExprTip);

                case ExpressionType.Parameter:
                    return (
                        new [] { new MappedProperty(expr as ParameterExpression, null, toPrefix, expr.Type) },
                        EmptyMappedTables,
                        false
                    );
                    
                case ExpressionType.AndAlso:
                case ExpressionType.OrElse:
                case ExpressionType.Add:
                case ExpressionType.Subtract:
                case ExpressionType.Multiply:
                case ExpressionType.Divide:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                    var asB = expr as BinaryExpression;
                    return BuildMapForBinaryCondition(state, asB.Left, asB.Right, asB.Type, asB.NodeType, toPrefix).AddT(false);
                    
                case ExpressionType.MemberAccess:
                    return BuildMapForMemberAccess(state, expr as MemberExpression, toPrefix).AddT(false);

                case ExpressionType.Block:
                    throw new InvalidProgramException("Unsure how to deal with the last return value");
                    // return (expr as BlockExpression).Expressions
                    //     .Select(ex => BuildMap(state, ex, MapType.Other, toPrefix))
                    //     .AggregateTuple2();

                case ExpressionType.New:
                    return BuildMapForConstructor(state, expr as NewExpression, nextMap, toPrefix: toPrefix).AddT(false);

                case ExpressionType.MemberInit:
                    return BuildMapForMemberInit(state, expr as MemberInitExpression, toPrefix).AddT(false);

                case ExpressionType.NewArrayInit:
                    return BuildMapForNewArray(state, expr as NewArrayExpression, toPrefix).AddT(false);

                case ExpressionType.ListInit:
                    return BuildMapForNewList(state, expr as ListInitExpression, toPrefix).AddT(false);

                case ExpressionType.Call:
                    var exprMethod = expr as MethodCallExpression;

                    var (isIn, inLhs, inRhs) = ReflectionUtils.IsIn(exprMethod);
                    if (isIn)
                        return BuildMapForIn(state, inLhs, inRhs, toPrefix, isExprTip).AddT(false);

                    var (isCount, countExpr) = ReflectionUtils.IsCount(expr);
                    if (isCount)
                        // .One(...) is invisible as far as nextMap is concerned
                        return BuildMapForCount(state, countExpr, nextMap, toPrefix);
                        
                    var oneExpr = ReflectionUtils.IsOne(exprMethod);
                    if (oneExpr != null)
                        // .One(...) is invisible as far as nextMap is concerned
                        return BuildMap(state, oneExpr, nextMap, toPrefix);
                        
                    var (isToList, enumerableL) = ReflectionUtils.IsToList(exprMethod);
                    if (isToList)
                        // .ToList(...) is invisible as far as nextMap is concerned
                        return BuildMap(state, enumerableL, nextMap, toPrefix);
                        
                    var (isToArray, enumerableA) = ReflectionUtils.IsToArray(exprMethod);
                    if (isToArray)
                        // .ToArray(...) is invisible as far as nextMap is concerned
                        return BuildMap(state, enumerableA, nextMap, toPrefix);

                    var (isSelect, enumerableS, mapper) = ReflectionUtils.IsSelectWithLambdaExpression(exprMethod);
                    if (isSelect)
                        return BuildMapForSelect(state, enumerableS, mapper, toPrefix, isExprTip).AddT(false);

                    break;

                default:
                    break;
            }

            throw BuildMappingError(expr);
        }

        static readonly HashSet<ExpressionType> InPlaceArrayCreation = new HashSet<ExpressionType>
        {
            ExpressionType.NewArrayBounds,
            ExpressionType.NewArrayInit,
            ExpressionType.ListInit
        };

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

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForConstructor(BuildMapState state, NewExpression expr, MapType nextMap, string toPrefix = null)
        {
            // TODO: relax this condition
            // The issue is within ObjectPropertyGraphBuilder, complex properties (or constructor args)
            // are defined by the inner simple properties. e.g. property name = OuterObj.InnerProp will create
            // an outer obj and an inner prop. But if inner prop is not set, then there is no reference to outer
            // obj either.
            // see also supporting tests:
            //      PropertyGraph_ReturnsMultipleComplexArgsWithNoSimpleProps_ReturnsCorrectOPG1
            //      PropertyGraph_ReturnsMultipleComplexArgsWithNoSimpleProps_ReturnsCorrectOPG2
            //      ReturnMultipleFromMap_PreMappedWithComplexProperty
            //      ReturnMultipleFromMap_PreMappedWithSimpleConstructorArg
            //      ReturnMultipleFromMap_PreMappedWithSimplePropertyAndSimpleConstructorArg
            //      SimpleMapReturningEmptyObject

            if (nextMap != MapType.MemberInit && expr.Arguments.Count == 0)
                throw new InvalidOperationException($"You cannot map to an object with has no data from table columns: {expr}.");

            return expr.Arguments
                .Select(ex => (type: ex.Type, map: BuildMap(state, ex, MapType.Other, toPrefix: null, isExprTip: true)))
                .Select((map, i) => (
                    map.Item2.properties.SelectMany(p => CreateContructorArg(p, map.type, i)), 
                    map.map.tables.Select(x => new MappedTable(x.From, CombineStrings(SqlStatementConstants.ConstructorArgs.BuildConstructorArg(i), x.To)))))
                .AggregateTuple2();

            IEnumerable<MappedProperty> CreateContructorArg(MappedProperty arg, Type argType, int argIndex)
            {
                var many = PropertyRepresentsTable(state, arg) ?
                    SplitMapOfComplexProperty(arg, argType) :
                    arg.ToEnumerable();

                return many.Select(q => new MappedProperty(
                    q.FromParams,
                    CombineStrings(toPrefix, CombineStrings(SqlStatementConstants.ConstructorArgs.BuildConstructorArg(argIndex), q.To)), 
                    q.MappedPropertyType,
                    constructorArgs: q.PropertySegmentConstructors.Prepend(expr.Constructor).ToArray()));
            }
        }

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForBinaryCondition(BuildMapState state, Expression left, Expression right, Type combinedType, ExpressionType combiner, string toPrefix = null) =>
            BuildMapForBinaryCondition(state, left, right, combinedType, combiner.ToCombinationType(), toPrefix);

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForBinaryCondition(BuildMapState state, Expression left, Expression right, Type combinedType, CombinationType combiner, string toPrefix = null)
        {
            var l = BuildMap(state, left, MapType.Other, toPrefix);
            var r = BuildMap(state, right, MapType.Other, toPrefix);

            var lProp = l.properties.ToArray();
            var rProp = r.properties.ToArray();

            if (lProp.Length != 1)
                throw BuildMappingError(left);

            if (rProp.Length != 1)
                throw BuildMappingError(right);

            return (
                new MappedProperty(
                    lProp[0].FromParams.Combine(rProp[0].FromParams, combiner),
                    null, 
                    combinedType).ToEnumerable(),
                l.tables.Concat(r.tables)
            );
        }

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForMemberAccess(BuildMapState state, MemberExpression expr, string toPrefix = null)
        {
            var result = BuildMap(state, expr.Expression, MapType.MemberAccess, toPrefix);
            var properties = result.properties.Enumerate();

            // cannot support (x + y).Value
            if (properties.Any(p => !p.FromParams.HasOneItemOnly))
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
            var bindings = expr.Bindings.OfType<MemberAssignment>();
            var mapType = bindings.Any() ? MapType.MemberInit : MapType.Other;

            return BuildMap(state, expr.NewExpression, mapType, toPrefix)
                .RemoveLastT()
                .ToEnumerableStruct()
                .Concat(bindings
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

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForIn(
            BuildMapState state, 
            Expression lhs, 
            Expression rhs, 
            string toPrefix = null, 
            bool isExprTip = false)
        {
            var (lP, lTab, _) = BuildMap(state, lhs, MapType.Other, null, false);
            var (rP, rTab, rConstant) = BuildMap(state, rhs, MapType.Other, null, false);

            rP = rP.Enumerate();
            lP = lP.Enumerate();

            if (lP.Count() != 1)
                throw new InvalidOperationException($"Invalid mapping statement: {lhs}");
            if (rP.Count() != 1)
                throw new InvalidOperationException($"Invalid mapping statement: {rhs}");

            var rProp = rP.First();
            var lProp = lP.First();

            // TODO: can I relax this condition?
            if (rProp.FromParams.GetEnumerable1()
                .Any(x => x.param != null && !x.param.StartsWith("@")))
            {
                throw new InvalidOperationException($"The values in an \"IN (...)\" clause must be a real parameter value. " + 
                    $"They cannot come from another table:\n{rhs}");
            }

            var rhsType = ReflectionUtils.RemoveConvert(rhs).NodeType;
            if (rConstant || !InPlaceArrayCreation.Contains(rhsType))
            {
                // TODO: this method will require find and replace in strings (inefficient)
                rProp = new MappedProperty(
                    // if there is only one parameter, it is an array and will need to be
                    // split into parts when rendering
                    rProp.FromParams.MapParamName(n => $"{n}{SqlStatementConstants.ParamArrayFlag}"),
                    rProp.To,
                    rProp.MappedPropertyType,
                    rProp.PropertySegmentConstructors);
            }

            var inPart = new MappedProperty(
                lProp.FromParams.Combine(rProp.FromParams, CombinationType.In),
                toPrefix,
                typeof(bool),
                lProp.PropertySegmentConstructors.Concat(rProp.PropertySegmentConstructors).ToArray());

            return (
                inPart.ToEnumerable(),
                lTab.Concat(rTab));
        }
        
        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForCount(
            BuildMapState state, 
            Expression enumerable, 
            MapType nextMap,
            string toPrefix = null, 
            bool isExprTip = false)
        {
            var (properties, tables) = BuildMap(state, enumerable, nextMap, toPrefix, isExprTip);
            tables = tables.Enumerate();

            return (
                properties.Select(WrapWithFunc),
                tables);

            MappedProperty WrapWithFunc(MappedProperty property)
            {
                if (property.FromParams.Next.Any())
                    throw BuildMappingError(enumerable);

                return new MappedProperty(
                    new Accumulator(property.FromParams.Map(Map)),
                    property.To,
                    property.MappedPropertyType,
                    property.PropertySegmentConstructors);
            }

            (ParameterExpression paramRoot, string param, bool isAggregate) Map((ParameterExpression paramRoot, string param, bool isAggregate) x)
            {
                var param = x.param;
                if (tables.Any(t => t.From == param))
                    param = $"{param}{SqlStatementConstants.RowIdName}";

                param = string.IsNullOrEmpty(param) ?
                    $"{SqlStatementConstants.OpenFunctionAlias}{state.SqlBuilder.CountFunctionName}" :
                    $"{x.param}.{SqlStatementConstants.OpenFunctionAlias}{state.SqlBuilder.CountFunctionName}";
                
                return (x.paramRoot, param, true);
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

            if (outerMapProperties.Length != 1 || !outerMapProperties[0].FromParams.HasOneItemOnly)
                throw new InvalidOperationException($"Mapping from \"{enumerable}\" is not supported.");

            return (
                innerMap.properties
                    .Select(m => new MappedProperty(
                        m.FromParams.MapParam(x => (
                            x.paramRoot ?? outerMapProperties[0].FromParams.First.paramRoot, 
                            x.paramRoot == null ? CombineStrings(outerMapProperties[0].FromParams.First.param, x.param) : x.param,
                            x.isAggregate)),
                        CombineStrings(outerMapProperties[0].To, m.To), 
                        m.MappedPropertyType, 
                        m.PropertySegmentConstructors)),
                outerMap.tables
                    .Concat(innerMap.tables)
                    .Concat(newTableMap)
            );
        }

        /// <summary>
        /// Build a condition for a new list expression
        /// </summary>
        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForNewArray(BuildMapState state, NewArrayExpression expr, string toPrefix) => 
            BuildMapForNewArray(state, expr.Expressions, toPrefix, expr.Type);

        /// <summary>
        /// Build a condition for a new list expression
        /// </summary>
        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForNewList(BuildMapState state, ListInitExpression expr, string toPrefix) => 
            BuildMapForNewArray(state, expr.Initializers.Select(GetFirstListAddParam), toPrefix, expr.Type);

        static Expression GetFirstListAddParam(ElementInit i)
        {
            if (i.Arguments.Count != 1)
                throw new InvalidOperationException("Invalid list initializer.");
            
            return i.Arguments[0];
        }

        /// <summary>
        /// Build a condition from a list of expressions
        /// </summary>
        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForNewArray(
            BuildMapState state, 
            IEnumerable<Expression> elements, 
            string toPrefix,
            Type outputType)
        {
            elements = elements.Enumerate();
            if (!elements.Any())
                return (Enumerable.Empty<MappedProperty>(), Enumerable.Empty<MappedTable>());

            return elements
                .Select(e => BuildMap(state, e, MapType.Other, null, false))
                .Aggregate(Combine)
                .RemoveLastT();

            (IEnumerable<MappedProperty>, IEnumerable<MappedTable>, bool) Combine(
                (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables, bool) x, 
                (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables, bool) y)
            {
                var xProps = x.properties.ToArray();
                var yProps = y.properties.ToArray();
                if (xProps.Length != 1 || yProps.Length != 1)
                    throw new InvalidOperationException($"Unsupported mapping expression \"{elements.JoinString(", ")}\".");

                var prop = xProps[0].FromParams.Combine(yProps[0].FromParams, CombinationType.Comma);
                return (
                    new MappedProperty(
                        prop,
                        toPrefix,
                        outputType,
                        xProps[0].PropertySegmentConstructors.Concat(yProps[0].PropertySegmentConstructors).ToArray()).ToEnumerable(),
                    x.tables.Concat(y.tables),
                    false);
            }
        }

        static bool PropertyRepresentsTable(BuildMapState state, MappedProperty property)
        {
            // a 2 part property (e.g. x + 1) cannot represent a table
            if (!property.FromParams.HasOneItemOnly)
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

        static string CombineStrings(string s1, string s2)
        {
            return s1 == null && s2 == null ?
                null :
                s1 != null && s2 != null ? 
                    $"{s1}.{s2}" :
                    $"{s1}{s2}";
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

        static InvalidOperationException BuildMappingError(Expression mapping) => new InvalidOperationException($"Unsupported mapping expression \"{mapping}\".");

        static (bool isSuccess, string name) CompileMemberName(Expression expr)
        {
            var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(expr);
            if (!isPropertyChain) return (false, null);

            return (true, chain.JoinString("."));
        }

        public enum MapType
        {
            Root = 1,
            Select,
            MemberAccess,
            MemberInit,
            Other
        }
    }
}