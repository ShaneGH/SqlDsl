
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.SqlBuilders;
using SqlDsl.SqlExpressions;
using SqlDsl.Utils;
using SqlDsl.Utils.Diagnostics;

namespace SqlDsl.Mapper
{
    class StringBasedMappedProperty : MappedProperty<StringBasedElement>
    {
        public readonly IEnumerable<string> MappingContext;

        public StringBasedMappedProperty(
            ISqlExpression<StringBasedElement> fromParams, 
            string to, 
            Type mappedPropertyType, 
            string mappingContext, 
            ConstructorInfo[] constructorArgs = null)
            : this(fromParams, to, mappedPropertyType, new [] { mappingContext }, constructorArgs)
        {
        }

        private StringBasedMappedProperty(
            ISqlExpression<StringBasedElement> fromParams, 
            string to, 
            Type mappedPropertyType, 
            IEnumerable<string> mappingContext, 
            ConstructorInfo[] constructorArgs)
            : base(fromParams, to, mappedPropertyType, constructorArgs)
        {
            MappingContext = mappingContext;
        }
        
        public StringBasedMappedProperty(
            ParameterExpression fromParamRoot, 
            string from, 
            string to, 
            Type mappedPropertyType, 
            string mappingContext,
            ConstructorInfo[] constructorArgs = null, 
            string aggregatedToTable = null)

            : this (new StringBasedSqlExpression(
                new Accumulator<StringBasedElement, BinarySqlOperator>(
                    new StringBasedElement(fromParamRoot, from, aggregatedToTable))), to, mappedPropertyType, mappingContext, constructorArgs)
        {
        }

        public QueryElementBasedMappedProperty Convert(BuildMapState state)
        {
            var mappingContext = MappingContext
                .Select(x => state.WrappedSqlStatement.Tables[x])
                .ToArray();

            return new QueryElementBasedMappedProperty(
                FromParams.Convert(state),
                To,
                MappedPropertyType,
                mappingContext,
                PropertySegmentConstructors);
        }

        public StringBasedMappedProperty With(
            string prependMappingContext = null,
            ISqlExpression<StringBasedElement> fromParams = null, 
            string to = null, 
            Type mappedPropertyType = null,
            ConstructorInfo[] constructorArgs = null)
        {
            var mappingCtxt = prependMappingContext == null
                ? MappingContext
                : MappingContext.Prepend(prependMappingContext);

            return new StringBasedMappedProperty(
                fromParams ?? FromParams, 
                to ?? To, 
                mappedPropertyType ?? MappedPropertyType,
                mappingCtxt,
                constructorArgs ?? PropertySegmentConstructors);
        }
    }
    
    class QueryElementBasedMappedProperty : MappedProperty<SelectColumnBasedElement>
    {
        public readonly IQueryTable[] MappingContext;

        public QueryElementBasedMappedProperty(ISqlExpression<SelectColumnBasedElement> fromParams, string to, Type mappedPropertyType, IQueryTable[] mappingContext, ConstructorInfo[] constructorArgs = null)
            : base(fromParams, to, mappedPropertyType, constructorArgs)
        {
            MappingContext = mappingContext;
        }
    }

    [DebuggerDisplay("{GetDebuggerDisplay()}")]
    class MappedProperty<TElement> : IDebuggerDisplay
    {
        static readonly ConstructorInfo[] EmptyConstructorArgs = new ConstructorInfo[0];

        public readonly Type MappedPropertyType;
        public readonly ConstructorInfo[] PropertySegmentConstructors;
        public readonly string To;
        public readonly ISqlExpression<TElement> FromParams;

        public MappedProperty(ISqlExpression<TElement> fromParams, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs)
        {
            To = to;
            FromParams = fromParams;
            MappedPropertyType = mappedPropertyType;
            PropertySegmentConstructors = constructorArgs ?? EmptyConstructorArgs;
        }
        
        public string GetDebuggerDisplay()
        {
            return $"{FromParams.GetDebuggerDisplay()} => {To}";
        }
    }
}