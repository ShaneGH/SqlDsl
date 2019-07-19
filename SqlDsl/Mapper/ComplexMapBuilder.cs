using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using SqlDsl.Query;
using SqlDsl.Schema;
using SqlDsl.SqlBuilders;
using SqlDsl.SqlExpressions;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    static class ComplexMapBuilder
    {
        static readonly IEnumerable<MappedTable> EmptyMappedTables = Enumerable.Empty<MappedTable>();

        public static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMap(BuildMapState state, Expression expr)
        {
            var (x, y, _) = BuildMapWithErrorHandling(state, expr, MapType.Root);
            return (x, y);
        }

        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables, bool isConstant) BuildMapWithErrorHandling(
            BuildMapState state, 
            Expression expr, 
            MapType nextMap,
            string toPrefix = null)
        {
            try
            {
                return BuildMap(state, expr, nextMap, toPrefix);
            }
            catch (Exception e)
            {
                throw new SqlBuilderException(state.MappingPurpose, expr, e);
            }
        }

        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables, bool isConstant) BuildMap(
            BuildMapState state, 
            Expression expr, 
            MapType nextMap,
            string toPrefix)
        {
            var (isConstant, requiresArgs) = ReflectionUtils.IsConstant(expr, state.ArgsObject);
            if (isConstant)
            {
                var result = requiresArgs ?
                    QueryArgAccessor.Create(state.ArgsObject, expr) :
                    ReflectionUtils.ExecuteExpression(expr);

                var paramName = state.Parameters.AddParam(result, expr.Type);

                return (
                    new StringBasedMappedProperty(
                        null, 
                        paramName, 
                        toPrefix, 
                        expr.Type, 
                        state.MappingContext.propertyName,
                        aggregatedToTable: state.MappingContext.propertyName).ToEnumerable(),
                    EmptyMappedTables,
                    true
                );
            }

            switch (expr.NodeType)
            {
                case ExpressionType.Convert:
                    return BuildMapWithErrorHandling(state, (expr as UnaryExpression).Operand, nextMap, toPrefix);

                case ExpressionType.Parameter:
                    return (
                        new [] 
                        { 
                            new StringBasedMappedProperty(
                                expr as ParameterExpression, 
                                null, 
                                toPrefix, 
                                expr.Type,
                                state.MappingContext.propertyName) 
                        },
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
                    
                case ExpressionType.Conditional:
                    var asC = expr as ConditionalExpression;
                    return CaseMapBuilder.BuildMap(state, asC, toPrefix).AddT(false);
                    
                case ExpressionType.MemberAccess:
                    return BuildMapForMemberAccess(state, expr as MemberExpression, nextMap, toPrefix);

                case ExpressionType.New:
                    return BuildMapForConstructor(state, expr as NewExpression, nextMap, toPrefix: toPrefix).AddT(false);

                case ExpressionType.MemberInit:
                    return BuildMapForMemberInit(state, expr as MemberInitExpression, nextMap, toPrefix).AddT(false);

                case ExpressionType.ArrayLength:
                    return BuildMapForCount(state, (expr as UnaryExpression).Operand, toPrefix).AddT(false);

                case ExpressionType.NewArrayInit:
                case ExpressionType.NewArrayBounds:
                    return BuildMapForNewArray(state, expr as NewArrayExpression, toPrefix).AddT(false);

                case ExpressionType.ListInit:
                    return BuildMapForNewList(state, expr as ListInitExpression, toPrefix).AddT(false);

                case ExpressionType.Call:
                    var exprMethod = expr as MethodCallExpression;

                    var isOrderByRowNumber = ReflectionUtils.IsOrderByRowNumber(exprMethod);
                    if (isOrderByRowNumber)
                        return BuildMapForOrderByRowNumber(state, exprMethod, toPrefix).AddT(false);

                    var (isIn, inLhs, inRhs) = ReflectionUtils.IsIn(exprMethod);
                    if (isIn)
                        return BuildMapForIn(state, inLhs, inRhs, toPrefix).AddT(false);

                    var (isCount, countExpr) = ReflectionUtils.IsCount(expr);
                    if (isCount)
                        return BuildMapForCount(state, countExpr, toPrefix).AddT(false);

                    var (isSum, sumExpr, sumMapper) = ReflectionUtils.IsSum(exprMethod);
                    if (isSum)
                        return sumMapper == null
                            ? BuildMapForSum(state, sumExpr, toPrefix).AddT(false)
                            : BuildMapForSum(state, sumExpr, sumMapper, toPrefix).AddT(false);

                    var (isAverage, averageExpr, averageMapper) = ReflectionUtils.IsAverage(exprMethod);
                    if (isAverage)
                        return averageMapper == null
                            ? BuildMapForAverage(state, averageExpr, toPrefix).AddT(false)
                            : BuildMapForAverage(state, averageExpr, averageMapper, toPrefix).AddT(false);

                    var (isMax, maxExpr, maxMapper) = ReflectionUtils.IsMax(exprMethod);
                    if (isMax)
                        return maxMapper == null
                            ? BuildMapForMax(state, maxExpr, toPrefix).AddT(false)
                            : BuildMapForMax(state, maxExpr, maxMapper, toPrefix).AddT(false);

                    var (isMin, minExpr, minMapper) = ReflectionUtils.IsMin(exprMethod);
                    if (isMin)
                        return minMapper == null
                            ? BuildMapForMin(state, minExpr, toPrefix).AddT(false)
                            : BuildMapForMin(state, minExpr, minMapper, toPrefix).AddT(false);
                        
                    var oneExpr = ReflectionUtils.IsOne(exprMethod);
                    if (oneExpr != null)
                        return BuildMapForOne(state, oneExpr, nextMap, toPrefix);
                        
                    var (isToList, enumerableL) = ReflectionUtils.IsToList(exprMethod);
                    if (isToList)
                        // .ToList(...) is invisible as far as nextMap is concerned
                        return BuildMapWithErrorHandling(state, enumerableL, nextMap, toPrefix);
                        
                    var (isToArray, enumerableA) = ReflectionUtils.IsToArray(exprMethod);
                    if (isToArray)
                        // .ToArray(...) is invisible as far as nextMap is concerned
                        return BuildMapWithErrorHandling(state, enumerableA, nextMap, toPrefix);

                    var (isSelect, enumerableS, mapper) = ReflectionUtils.IsSelectWithLambdaExpression(exprMethod);
                    if (isSelect)
                        return BuildMapForSelect(state, enumerableS, nextMap, mapper, toPrefix).AddT(false);

                    var isCase = CaseMapBuilder.IsCase(exprMethod);
                    if (isCase)
                        return CaseMapBuilder.BuildMap(state, exprMethod, nextMap, toPrefix).AddT(false);

                    var isSimpleCase = SimpleCaseMapBuilder.IsCase(exprMethod);
                    if (isSimpleCase)
                        return SimpleCaseMapBuilder.BuildMap(state, exprMethod, nextMap, toPrefix).AddT(false);

                    break;

                default:
                    break;
            }

            throw BuildMappingError(state.MappingPurpose, expr);
        }

        static readonly HashSet<ExpressionType> InPlaceArrayCreation = new HashSet<ExpressionType>
        {
            ExpressionType.NewArrayBounds,
            ExpressionType.NewArrayInit,
            ExpressionType.ListInit
        };

        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForConstructor(BuildMapState state, NewExpression expr, MapType nextMap, string toPrefix = null)
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
                throw BuildMappingError(state.MappingPurpose, $"You cannot map to an object with has no data from table columns: {expr}.");

            return expr.Arguments
                .Select((ex, i) => (type: ex.Type, map: BuildMapWithErrorHandling(state, ex, MapType.Other, toPrefix: CombineStrings(toPrefix, SqlStatementConstants.ConstructorArgs.BuildConstructorArg(i)))))
                .Select((map, i) => (
                    map.Item2.properties.SelectMany(p => CreateContructorArg(p, map.type, i)), 
                    map.map.tables.Select(x => new MappedTable(x.From, CombineStrings(SqlStatementConstants.ConstructorArgs.BuildConstructorArg(i), x.To), x.TableresultsAreAggregated))))
                .AggregateTuple2();

            IEnumerable<StringBasedMappedProperty> CreateContructorArg(StringBasedMappedProperty arg, Type argType, int argIndex)
            {
                var many = PropertyRepresentsTable(state, arg) ?
                    SplitMapOfComplexProperty(state, arg, argType) :
                    arg.ToEnumerable();

                return many.Select(q => q.With(
                    constructorArgs: q.PropertySegmentConstructors.Prepend(expr.Constructor).ToArray()));
            }
        }

        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForBinaryCondition(BuildMapState state, Expression left, Expression right, Type combinedType, ExpressionType combiner, string toPrefix = null) =>
            BuildMapForBinaryCondition(state, left, right, combinedType, combiner.ToCombinationType(), toPrefix);

        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForBinaryCondition(BuildMapState state, Expression left, Expression right, Type combinedType, BinarySqlOperator combiner, string toPrefix = null)
        {
            var l = BuildMapWithErrorHandling(state, left, MapType.Other, toPrefix);
            var r = BuildMapWithErrorHandling(state, right, MapType.Other, toPrefix);

            var lProp = l.properties.ToArray();
            var rProp = r.properties.ToArray();

            if (lProp.Length > 1 || rProp.Length > 1)
                throw BuildMappingError(state.MappingPurpose, $"You cannot perform a \"{combiner}\" operation on a table");

            if (lProp.Length != 1)
                throw BuildMappingError(state.MappingPurpose, left);

            if (rProp.Length != 1)
                throw BuildMappingError(state.MappingPurpose, right);

            return (
                new StringBasedMappedProperty(
                    lProp[0].FromParams.Combine(rProp[0].FromParams, combiner),
                    toPrefix, 
                    combinedType,
                    state.MappingContext.propertyName).ToEnumerable(),
                l.tables.Concat(r.tables)
            );
        }

        static bool MemberAccessExpressionNeedsExpansion(BuildMapState state, MemberExpression expr, MapType nextMapType)
        {
            switch (nextMapType)
            {
                case MapType.MemberAccess:
                case MapType.Select:
                case MapType.AggregateFunction:
                case MapType.RowNumber:
                    return false;
            }
            
            var (isPropertyChain, root, chain) = ReflectionUtils.GetPropertyChain(expr, allowOne: false, allowConstants: false, allowSelect: true);
            if (!isPropertyChain)
                return false;
            
            if (!(root is ParameterExpression))
                return false;
                
            var (param, _) = new StringBasedElement(root as ParameterExpression, chain.JoinString("."), null)
                .AddRoot(state);

            return state.WrappedSqlStatement.Tables.TryGetTable(param) != null;
        }

        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables, bool isConstant) BuildMapForMemberAccess(BuildMapState state, MemberExpression expr, MapType nextMapType, string toPrefix = null)
        {
            var (isCount, enumerable) = ReflectionUtils.IsCount(expr);
            if (isCount)
                return BuildMapForCount(state, enumerable, toPrefix).AddT(false);

            if (MemberAccessExpressionNeedsExpansion(state, expr, nextMapType))
            {
                var rewritten = ReflectionUtils.GetIEnumerableType(expr.Type) == null
                    ? ReflectionUtils.ConvertToFullMemberInit(expr)
                    : ReflectionUtils.ConvertCollectionToFullMemberInit(expr);

                return BuildMapWithErrorHandling(state, rewritten, nextMapType, toPrefix);
            }

            var result = BuildMapWithErrorHandling(state, expr.Expression, MapType.MemberAccess, toPrefix);
            var properties = result.properties.Enumerate();

            // cannot support (x + y).Value
            if (properties.Any(p => !p.FromParams.HasOneItemOnly))
                throw BuildMappingError(state.MappingPurpose, expr);

            return (
                result.properties
                    .Select(p => p.With( 
                        fromParams: p.FromParams.MapParamName(x => CombineStrings(x, GetMemberName(expr.Member))),
                        mappedPropertyType: expr.Type))
                    .Enumerate(),
                result.tables,
                result.isConstant
            );

            string GetMemberName(MemberInfo member)
            {
                if (state.UseColumnAliases)
                    return member.Name;

                return ColumnAttribute.GetColumnName(member).name;
            }
        }

        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForMemberInit(BuildMapState state, MemberInitExpression expr, MapType nextMap, string toPrefix = null)
        {
            var bindings = expr.Bindings.OfType<MemberAssignment>();
            var mapType = bindings.Any() ? MapType.MemberInit : MapType.Other;

            return BuildMapWithErrorHandling(state, expr.NewExpression, mapType, toPrefix)
                .RemoveLastT()
                .ToEnumerableStruct()
                .Concat(bindings
                    .Select(b => (binding: b, memberName: b.Member.Name, map: BuildMapWithErrorHandling(state, b.Expression, MapType.Other, b.Member.Name)))
                    .Select(m => (
                        m.map.properties.SelectMany(x => PropertyRepresentsTable(state, x)
                            ? SplitMapOfComplexProperty(state, x, m.binding.Member.GetPropertyOrFieldType())
                            : x.With(
                                to: CombineStrings(toPrefix, x.To)).ToEnumerable()),
                        m.map.tables.Select(x => new MappedTable(x.From, CombineStrings(m.memberName, x.To), x.TableresultsAreAggregated)))))
                .AggregateTuple2();
        }

        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForOrderByRowNumber(BuildMapState state, MethodCallExpression rowNumberExpression, string toPrefix)
        {
            var result = new StringBasedMappedProperty(
                null, 
                SqlStatementConstants.OrderByRowIdName, 
                toPrefix, 
                rowNumberExpression.Type, 
                state.MappingContext.propertyName);

            return (
                result.ToEnumerable(),
                CodingConstants.Empty.MappedTable);
        }

        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForIn(
            BuildMapState state, 
            Expression lhs, 
            Expression rhs, 
            string toPrefix = null)
        {
            var (lP, lTab, _) = BuildMapWithErrorHandling(state, lhs, MapType.Other, null);
            var (rP, rTab, rConstant) = BuildMapWithErrorHandling(state, rhs, MapType.Other, null);

            rP = rP.Enumerate();
            lP = lP.Enumerate();

            if (lP.Count() != 1)
                throw BuildMappingError(state.MappingPurpose, lhs);
            if (rP.Count() != 1)
                throw BuildMappingError(state.MappingPurpose, rhs);

            var rProp = rP.First();
            var lProp = lP.First();

            // TODO: can I relax this condition?
            if (rProp.FromParams.GetEnumerable()
                .Any(x => x.Param != null && !x.Param.StartsWith("@")))
            {
                throw BuildMappingError(state.MappingPurpose, $"The values in an \"IN (...)\" clause must be a real parameter value. " + 
                    $"They cannot come from another table:\n{rhs}");
            }

            var rhsType = ReflectionUtils.RemoveConvert(rhs).NodeType;
            if (rConstant || !InPlaceArrayCreation.Contains(rhsType))
            {
                // TODO: this method will require find and replace in strings (inefficient)
                rProp = rProp.With( 
                    // if there is only one parameter, it is an array and will need to be
                    // split into parts when rendering
                    fromParams: rProp.FromParams.MapParamName(n => $"{n}{SqlStatementConstants.ParamArrayFlag}"));
            }

            var inPart = new StringBasedMappedProperty(
                lProp.FromParams.Combine(rProp.FromParams, BinarySqlOperator.In),
                toPrefix,
                typeof(bool), 
                state.MappingContext.propertyName,
                lProp.PropertySegmentConstructors.Concat(rProp.PropertySegmentConstructors).ToArray());

            return (
                inPart.ToEnumerable(),
                lTab.Concat(rTab));
        }
        
        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForCount(
            BuildMapState state, 
            Expression enumerable, 
            string toPrefix = null) => BuildMapForAggregate(state, enumerable, true, UnarySqlOperator.CountFunction, toPrefix);
        
        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForSum(
            BuildMapState state, 
            Expression enumerable, 
            string toPrefix = null) => BuildMapForAggregate(state, enumerable, false, UnarySqlOperator.SumFunction, toPrefix);
        
        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForSum(
            BuildMapState state, 
            Expression enumerable, 
            LambdaExpression sumMapper, 
            string toPrefix = null) => BuildMapForSum(state, AddSelectToEnunmerable(state, enumerable, sumMapper), toPrefix);
        
        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForAverage(
            BuildMapState state, 
            Expression enumerable, 
            string toPrefix = null) => BuildMapForAggregate(state, enumerable, false, UnarySqlOperator.AverageFunction, toPrefix);
        
        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForAverage(
            BuildMapState state, 
            Expression enumerable, 
            LambdaExpression averageMapper, 
            string toPrefix = null) => BuildMapForAverage(state, AddSelectToEnunmerable(state, enumerable, averageMapper), toPrefix);
        
        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForMax(
            BuildMapState state, 
            Expression enumerable, 
            string toPrefix = null) => BuildMapForAggregate(state, enumerable, false, UnarySqlOperator.MaxFunction, toPrefix);
        
        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForMax(
            BuildMapState state, 
            Expression enumerable, 
            LambdaExpression averageMapper, 
            string toPrefix = null) => BuildMapForMax(state, AddSelectToEnunmerable(state, enumerable, averageMapper), toPrefix);
        
        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForMin(
            BuildMapState state, 
            Expression enumerable, 
            string toPrefix = null) => BuildMapForAggregate(state, enumerable, false, UnarySqlOperator.MinFunction, toPrefix);
        
        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForMin(
            BuildMapState state, 
            Expression enumerable, 
            LambdaExpression averageMapper, 
            string toPrefix = null) => BuildMapForMin(state, AddSelectToEnunmerable(state, enumerable, averageMapper), toPrefix);

        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables, bool isConstant) BuildMapForOne(
            BuildMapState state, 
            Expression expr, 
            MapType nextMap,
            string toPrefix = null)
        {
            // if relaxing this condition, re-enable test: GetOneTableAndOrderByOnProperty
            // also look at the skipOrderBy parameter in ConditionAndMappingParts test
            if (state.MappingPurpose == MappingPurpose.OrderBy)
                throw BuildMappingError(state.MappingPurpose, "You cannot order by a column in a one to many relatiionship.");

            // .One(...) is invisible as far as nextMap is concerned
            return BuildMapWithErrorHandling(state, expr, nextMap, toPrefix: toPrefix);
        }
        
        /// <summary>
        /// convert xs
        /// to
        /// convert xs.Select(x => x.val)
        /// </summary>
        static Expression AddSelectToEnunmerable(BuildMapState state, Expression enumerable, LambdaExpression mapper) 
        {
            var enumeratedType = ReflectionUtils.GetIEnumerableType(enumerable.Type);
            if (enumeratedType == null)
                throw BuildMappingError(state.MappingPurpose, enumerable);

            return Expression.Call(
                CodingConstants.GenericSelectMethod.MakeGenericMethod(enumeratedType, mapper.Body.Type),
                enumerable,
                mapper);
        }
            
        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForAggregate(
            BuildMapState state, 
            Expression enumerable,
            bool canSubstituteRowNumberForTable,
            UnarySqlOperator function,
            string toPrefix = null)
        {
            if (state.MappingPurpose == MappingPurpose.OrderBy)
                throw BuildMappingError(state.MappingPurpose, "You cannot use aggregate functions in an order by clause.");

            var (properties, tables, _) = BuildMapWithErrorHandling(state, enumerable, MapType.AggregateFunction, toPrefix);

            // if aggregate is on a table, change to aggregate row id
            properties = properties.Select(arg => arg.With( 
                fromParams: PropertyRepresentsTable(state, arg)
                    ? canSubstituteRowNumberForTable
                        ? arg.FromParams.MapParamName(x => $"{x}.{SqlStatementConstants.PrimaryKeyName}0")
                        : ThrowMappingError<ISqlExpression<StringBasedElement>>(state.MappingPurpose, $". Cannot apply {function.ToString()} function to table \"{enumerable}\".")
                    : arg.FromParams,
                mappedPropertyType: typeof(int)));

            tables = tables.Enumerate();

            return (
                properties.Select(WrapWithFunc),
                tables);

            StringBasedMappedProperty WrapWithFunc(StringBasedMappedProperty property)
            {
                return property.With(
                    fromParams: new UnarySqlExpression<StringBasedElement>(property.FromParams, function));
            }
        }

        static readonly Func<ParameterExpression, IDisposable> DefaultOnContextNotFound = newContext => 
            throw new SqlBuilderException($"You cannot use a Select(...) on a field.");

        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForSelect(BuildMapState state, Expression enumerable, MapType nextMap, LambdaExpression mapper, string toPrefix)
        {
            // if relaxing this condition, re-enable test: SelectFromTableAndOrderByResult
            // also look at the skipOrderBy parameter in ConditionAndMappingParts test
            if (state.MappingPurpose == MappingPurpose.OrderBy)
                throw BuildMappingError(state.MappingPurpose, "You cannot order by a column in a one to many relationship.");

            if (mapper.Body == mapper.Parameters[0])
            {
                throw BuildMappingError(state.MappingPurpose, mapper);
            }

            TryAddSelectStatementParameterToProperty(state, enumerable, mapper.Parameters[0]);

            var outerMap = BuildMapWithErrorHandling(state, enumerable, MapType.Select, toPrefix);

            (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables, bool) innerMap;
            using (state.SwitchContext(mapper.Parameters[0], DefaultOnContextNotFound))
                innerMap = BuildMapWithErrorHandling(state, mapper.Body, MapType.ContextSwitch);

            var (isSuccess, name) = CompileMemberName(enumerable);
            var newTableMap = isSuccess
                ? new MappedTable(name, null, nextMap == MapType.AggregateFunction).ToEnumerable()
                : EmptyMappedTables;

            var outerMapProperties  = outerMap.properties.ToArray();
            if (outerMapProperties.Length != 1 || !outerMapProperties[0].FromParams.HasOneItemOnly)
                throw BuildMappingError(state.MappingPurpose, enumerable);

            return (
                innerMap.properties
                    .Select(m => m.With( 
                        prependMappingContext: state.MappingContext.propertyName,
                        fromParams: m.FromParams.MapParam(x => new StringBasedElement(
                            x.ParamRoot ?? outerMapProperties[0].FromParams.First.ParamRoot, 
                            x.ParamRoot != null || (x.Param?.StartsWith("@") ?? false)
                                ? x.Param
                                : CombineStrings(outerMapProperties[0].FromParams.First.Param, x.Param),
                            x.AggregatedToTable)),
                        to: CombineStrings(outerMapProperties[0].To, m.To))),
                outerMap.tables
                    .Concat(innerMap.tables)
                    .Concat(newTableMap)
            );
        }

        /// <summary>
        /// Build a condition for a new list expression
        /// </summary>
        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForNewArray(BuildMapState state, NewArrayExpression expr, string toPrefix) => 
            BuildMapForNewArray(state, expr.Expressions, toPrefix, expr.Type);

        /// <summary>
        /// Build a condition for a new list expression
        /// </summary>
        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForNewList(BuildMapState state, ListInitExpression expr, string toPrefix)
        {
            return BuildMapForNewArray(state, expr.Initializers.Select(GetFirstListAddParam), toPrefix, expr.Type);

            Expression GetFirstListAddParam(ElementInit i)
            {
                if (i.Arguments.Count != 1)
                    throw BuildMappingError(state.MappingPurpose, "Invalid list initializer.");
                
                return i.Arguments[0];
            }
        }

        /// <summary>
        /// Build a condition from a list of expressions
        /// </summary>
        static (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForNewArray(
            BuildMapState state, 
            IEnumerable<Expression> elements, 
            string toPrefix,
            Type outputType)
        {
            elements = elements.Enumerate();
            if (!elements.Any())
                return (Enumerable.Empty<StringBasedMappedProperty>(), Enumerable.Empty<MappedTable>());

            return elements
                .Select(e => BuildMapWithErrorHandling(state, e, MapType.Other, null))
                .Aggregate(Combine)
                .RemoveLastT();

            (IEnumerable<StringBasedMappedProperty>, IEnumerable<MappedTable>, bool) Combine(
                (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables, bool) x, 
                (IEnumerable<StringBasedMappedProperty> properties, IEnumerable<MappedTable> tables, bool) y)
            {
                var xProps = x.properties.ToArray();
                var yProps = y.properties.ToArray();
                if (xProps.Length != 1 || yProps.Length != 1)
                    throw BuildMappingError(state.MappingPurpose, $"Unsupported mapping expression \"{elements.JoinString(", ")}\".");

                var prop = xProps[0].FromParams.Combine(yProps[0].FromParams, BinarySqlOperator.Comma);
                return (
                    new StringBasedMappedProperty(
                        prop,
                        toPrefix,
                        outputType, 
                        state.MappingContext.propertyName,
                        xProps[0].PropertySegmentConstructors.Concat(yProps[0].PropertySegmentConstructors).ToArray()).ToEnumerable(),
                    x.tables.Concat(y.tables),
                    false);
            }
        }

        static bool PropertyRepresentsTable(BuildMapState state, StringBasedMappedProperty property)
        {
            // a 2 part property (e.g. x + 1) cannot represent a table
            if (!property.FromParams.HasOneItemOnly)
                return false;

            // mapped property points to the root query object
            if (property.FromParams.First.Param == null && property.FromParams.First.ParamRoot == state.QueryObject)
                return true;

            // mapped property points to a table on the query object
            if (property.FromParams.First.ParamRoot == state.QueryObject && state.WrappedSqlStatement.ContainsTable(property.FromParams.First.Param))
                return true;

            // mapped property points to a table on the query object
            if (property.FromParams.First.Param == null && state.ParameterRepresentsProperty.Any(p => p.parameter == property.FromParams.First.ParamRoot))
                return true;

            return false;
        }

        static IEnumerable<StringBasedMappedProperty> SplitMapOfComplexProperty(BuildMapState state, StringBasedMappedProperty property, Type propertyType)
        {
            if (state == null)
                throw new ArgumentNullException(nameof(state));
            if (property == null)
                throw new ArgumentNullException(nameof(property));
            if (propertyType == null)
                throw new ArgumentNullException(nameof(propertyType));

            var param = Expression.Parameter(propertyType);
            state.ParameterRepresentsProperty.Add((param, property.To.Split('.')));

            using (state.SwitchContext(param))
            {
                propertyType = ReflectionUtils.GetIEnumerableType(propertyType) ?? propertyType;
                return propertyType
                    .GetFieldsAndProperties()
                    .Select(mem => new StringBasedMappedProperty(
                        property.FromParams.MapParamName(x => CombineStrings(x, mem.name)), 
                        CombineStrings(property.To, mem.name), 
                        mem.type, 
                        state.MappingContext.propertyName));
            }
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

        static SqlBuilderException BuildMappingError(MappingPurpose purpose, Expression expr) => new SqlBuilderException(purpose, expr);

        static SqlBuilderException BuildMappingError(MappingPurpose purpose, string message) => new SqlBuilderException(purpose, message);

        static T ThrowMappingError<T>(MappingPurpose purpose, string message) => throw BuildMappingError(purpose, message);

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
            AggregateFunction,
            ContextSwitch,
            RowNumber,
            Other
        }
    }
}