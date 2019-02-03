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

        public static (IEnumerable<QueryElementBasedMappedProperty> properties, IEnumerable<StrongMappedTable> tables) BuildMapFromRoot(BuildMapState state, Expression expression)
        {
            var (properties, tables) = ComplexMapBuilder.BuildMap(state, expression);
            
            return (
                properties.Select(p => p.Convert(state)).ToArray(), 
                tables.Select(p => p.Convert(state.WrappedSqlStatement)).ToArray());
        }

        public class ExpressionMappingTypeFinder : ExpressionVisitor, IDisposable
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

            public static MappingType_New GetMappingType(Expression expression)
            {
                using (Instance)
                {
                    Instance.Visit(expression);
                    if (Instance.IsMap)
                        return MappingType_New.Map;
                }
                
                return MappingType_New.SingleProp;
            }
        }
        
        /// <summary>
        /// The type of mapping to be performed.
        /// The results are ordered to hace the simplest mapping first, down to the most complex
        /// </summary>
        public enum MappingType_New
        {
            SingleProp = 1,
            Map,
        }
    }
}