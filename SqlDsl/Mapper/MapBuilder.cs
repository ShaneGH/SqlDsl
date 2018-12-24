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

        public static (MappingType resultType, IEnumerable<MappedProperty> properties, IEnumerable<MappedTable> tables) BuildMapFromRoot(BuildMapState state, Expression expression)
        {
            var (properties, tables) = ComplexMapBuilder.BuildMap(state, expression);
            var ps = properties.ToArray();
            
            return (ExpressionMappingTypeFinder.GetMappingType(ps, expression, state), ps, tables);

            /*
            if (mappedChains.Any(c => c.result.FromParams.GetEnumerable1().Any(x => x.isAggregate)))
                throw new NotImplementedException("Cannot handle simple aggregate result"); // TODO

            if (mappedChains.Length == 0)
                throw new InvalidOperationException("Unable to understand mapping statement: " + expression); */
        }

        class ExpressionMappingTypeFinder : ExpressionVisitor, IDisposable
        {
            [ThreadStatic]
            static readonly ExpressionMappingTypeFinder Instance = new ExpressionMappingTypeFinder();

            bool IsMap;

            private ExpressionMappingTypeFinder()
            {
                Init();
            }

            public void Dispose() => Init();

            void Init()
            {
                IsMap = false;
            }

            public override Expression Visit(Expression node)
            {
                // early out if map has been found
                if (IsMap)
                    return node;
                    
                return base.Visit(node);
            }
            
            protected override Expression VisitNew(NewExpression node)
            {
                if (node.Arguments.Count == 0)
                    return base.VisitNew(node);
                    
                IsMap = true;
                return node;
            }
            
            protected override Expression VisitMemberInit(MemberInitExpression node)
            {
                IsMap = true;
                return node;
            }

            public static MappingType GetMappingType(MappedProperty[] properties, Expression expression, BuildMapState state)
            {
                if (properties.Length == 0)
                    return MappingType.SimpleProp;

                using (Instance)
                {
                    Instance.Visit(expression);
                    if (Instance.IsMap)
                        return MappingType.Map;
                }

                if (properties.Length != 1 || properties.Any(p => p.To != null))
                {
                    return ReflectionUtils.GetIEnumerableType(expression.Type) == null ?
                        MappingType.SingleComplexProp :
                        MappingType.MultiComplexProp;
                }
                
                var fromParams = properties[0].FromParams
                    .GetEnumerable1()
                    .Select(x => Accumulator.AddRoot(x, state))
                    .ToArray();

                foreach (var table in state.WrappedSqlStatement.Tables)
                {
                    if (fromParams.Any(p => p.param == table.Alias))
                    {
                        return ReflectionUtils.GetIEnumerableType(expression.Type) == null ?
                            MappingType.SingleComplexProp :
                            MappingType.MultiComplexProp;
                    }
                }

                return MappingType.SimpleProp;
            }
        }
        
        /// <summary>
        /// The type of mapping to be performed.
        /// The results are ordered to hace the simplest mapping first, down to the most complex
        /// </summary>
        public enum MappingType
        {
            SimpleProp = 1,
            SingleComplexProp,
            MultiComplexProp,
            Map,
        }
    }
}