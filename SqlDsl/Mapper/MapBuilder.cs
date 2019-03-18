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

        public static MappingType GetMappingType(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Convert)
                expression = (expression as UnaryExpression).Operand;

            switch (expression.NodeType)
            {
                case ExpressionType.New:
                case ExpressionType.MemberInit:
                    return MappingType.Map;
                default:
                    return MappingType.SingleProp;
            }
        }
        
        /// <summary>
        /// The type of mapping to be performed.
        /// The results are ordered to hace the simplest mapping first, down to the most complex
        /// </summary>
        public enum MappingType
        {
            SingleProp = 1,
            Map,
        }
    }
}