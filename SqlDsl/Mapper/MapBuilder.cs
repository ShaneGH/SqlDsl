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
                return BuildMapForPropertyChain(state, expression, chains);

            var (properties, tables) = ComplexMapBuilder.BuildMap(state, expression);

            // if expression returns an object (e.g. a => a) the To param might be null
            tables = tables.Where(t => t.To != null);
            return (BuildMapResult.Map, properties, tables);
        }

        static (BuildMapResult resultType, IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapForPropertyChain(
            BuildMapState state, 
            Expression expression,
            StructAccumulator<(Expression root, IEnumerable<string> chain, Expression fullExpression), ExpressionType> chains)
        {
            var mappedChains = chains
                .GetEnumerable2()
                .Select(c => BuildMapForOneAccumulatorBranch(state, chains.Next.Any(), c.Item1.root, c.Item1.chain, c.Item1.fullExpression, c.Item2))
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

        static (ExpressionType? combiner, BuildMapResult type, MappedProperty result) BuildMapForOneAccumulatorBranch(BuildMapState state, bool moreThanOneBranch, Expression root, IEnumerable<string> chain, Expression fullExpression, ExpressionType? combiner)
        {
            if (root is ConstantExpression)
                return BuildMapForConstant(state, root, chain, combiner);

            var rootParam = root as ParameterExpression;
            if (rootParam == null)
                throw new InvalidOperationException($"Invalid root param type: {root}.");

            if (rootParam == state.ArgsObject)
                return BuildMapForArgs(state, fullExpression, combiner);

            if (rootParam != state.QueryObject)
                throw new InvalidOperationException("Unable to understand mapping statement: " + fullExpression);

            var pChain = chain.JoinString(".");
            if (pChain == "")
                throw new InvalidOperationException("You must provide a valid mapping with the Map(...) method.");

            foreach (var property in state.WrappedSqlStatement.Tables)
            {
                var result = TryBuildMapForTableProperty(property, state, moreThanOneBranch, rootParam, pChain, fullExpression, combiner);
                if (result.HasValue)
                    return result.Value;
            }

            // TODO: there is a third case where mapping expr is x => x.Outer (and Outer is {Inner: {Val: string}}
            throw new InvalidOperationException("Unable to understand mapping statement: " + fullExpression);
        }

        static (ExpressionType? combiner, BuildMapResult type, MappedProperty result) BuildMapForArgs(BuildMapState state, Expression fullExpression, ExpressionType? combiner)
        {
            var result = QueryArgAccessor.Create(state.ArgsObject, fullExpression);
            var paramName = state.Parameters.AddParam(result);

            return (
                combiner,
                BuildMapResult.SimpleProp,
                new MappedProperty(null, paramName, null, fullExpression.Type)
            );
        }

        static (ExpressionType? combiner, BuildMapResult type, MappedProperty result) BuildMapForConstant(BuildMapState state, Expression root, IEnumerable<string> chain, ExpressionType? combiner)
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
                combiner,
                BuildMapResult.SimpleProp,
                new MappedProperty(null, paramName, null, chainExpr.Type)
            );
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

        static (ExpressionType? combiner, BuildMapResult type, MappedProperty result)? TryBuildMapForTableProperty(IQueryTable property, BuildMapState state, bool moreThanOneBranch, ParameterExpression rootParam, string chain, Expression fullExpression, ExpressionType? combiner)
        {
            // get property from one table (no query object)
            if (property.Alias == SqlStatementConstants.RootObjectAlias && !chain.Contains("."))
                return BuildMapForSimpleProperty(state, rootParam, chain, fullExpression, combiner);

            if (chain == property.Alias)
            {
                // cannot support (1 + new { ... })
                if (moreThanOneBranch)
                    throw new InvalidOperationException("Unable to understand mapping statement: " + fullExpression);

                return BuildMapForTable(rootParam, chain, fullExpression, combiner);
            }

            if (chain.StartsWith(property.Alias) && 
                chain.Length >= (property.Alias.Length + 2) &&
                chain[property.Alias.Length] == '.' &&
                !chain.Substring(property.Alias.Length + 1).Contains('.'))
                
                return BuildMapForTableProperty(state, rootParam, chain, fullExpression, combiner);

            return null;
        }

        static (ExpressionType? combiner, BuildMapResult type, MappedProperty result) BuildMapForSimpleProperty(BuildMapState state, ParameterExpression rootParam, string chain, Expression fullExpression, ExpressionType? combiner)
        {
            return (
                combiner,
                BuildMapResult.SimpleProp,
                new MappedProperty(rootParam, chain, null, GetSimplePropertyCellType(fullExpression, state.QueryObject, 1))
            );
        }

        static (ExpressionType? combiner, BuildMapResult type, MappedProperty result) BuildMapForTable(ParameterExpression rootParam, string chain, Expression fullExpression, ExpressionType? combiner)
        {
            var resultType = ReflectionUtils.GetIEnumerableType(fullExpression.Type) == null ?
                BuildMapResult.SingleComplexProp :
                BuildMapResult.MultiComplexProp;
                
            return (
                combiner,
                resultType,
                new MappedProperty(rootParam, chain, null, fullExpression.Type)
            );
        }

        static (ExpressionType? combiner, BuildMapResult type, MappedProperty result) BuildMapForTableProperty(BuildMapState state, ParameterExpression rootParam, string chain, Expression fullExpression, ExpressionType? combiner)
        {
            return (
                combiner,
                BuildMapResult.SimpleProp,
                new MappedProperty(rootParam, chain, null, GetSimplePropertyCellType(fullExpression, state.QueryObject, 2))
            );
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