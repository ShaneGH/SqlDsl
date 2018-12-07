using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    static class ComplexMapBuilder
    {
        static readonly IEnumerable<MappedTable> EmptyMappedTables = Enumerable.Empty<MappedTable>();

        public static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMap(BuildMapState state, Expression expr)
        {
            return BuildMap(state, expr, MapType.Root, isExprTip: true);
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
                case ExpressionType.Subtract:
                case ExpressionType.Multiply:
                case ExpressionType.Divide:
                    var asB = expr as BinaryExpression;
                    return BuildMapForBinaryCondition(state, asB.Left, asB.Right, asB.Type, asB.NodeType, toPrefix);
                    
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

        static (IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForBinaryCondition(BuildMapState state, Expression left, Expression right, Type combinedType, ExpressionType combiner, string toPrefix = null)
        {
            var l = BuildMap(state, left, MapType.Other, toPrefix);
            var r = BuildMap(state, right, MapType.Other, toPrefix);

            var lProp = l.properties.ToArray();
            var rProp = r.properties.ToArray();

            if (lProp.Length != 1)
                throw new InvalidOperationException($"Unsupported mapping expression \"{left}\".");

            if (rProp.Length != 1)
                throw new InvalidOperationException($"Unsupported mapping expression \"{right}\".");

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
            Other
        }
    }
}