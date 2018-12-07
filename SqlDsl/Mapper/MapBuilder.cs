using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    static class MapBuilder
    {
        static readonly IEnumerable<MappedTable> EmptyMappedTables = Enumerable.Empty<MappedTable>();

        public static (BuildMapResult resultType, IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapFromRoot(BuildMapState state, Expression expression)
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

            var (properties, tables) = ComplexMapBuilder.BuildMap(state, expression);

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
        
        public enum BuildMapResult
        {
            Map = 1,
            SimpleProp,
            SingleComplexProp,
            MultiComplexProp
        }
    }
}