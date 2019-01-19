
using System;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using SqlDsl.Utils.Diagnostics;

namespace SqlDsl.Mapper
{
    class StringBasedMappedProperty : MappedProperty<StringBasedElement>
    {
        public StringBasedMappedProperty(IAccumulator<StringBasedElement> fromParams, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs = null)
            : base(fromParams, to, mappedPropertyType, constructorArgs)
        {
        }
        
        public StringBasedMappedProperty(ParameterExpression fromParamRoot, string from, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs = null, string aggregatedToTable = null)
            : this (new Accumulator<StringBasedElement>(
                new Accumulator<StringBasedElement, BinarySqlOperator>(
                    new StringBasedElement(fromParamRoot, from, aggregatedToTable))), to, mappedPropertyType, constructorArgs)
        {
        }

        public QueryElementBasedMappedProperty Convert(BuildMapState state)
        {
            return new QueryElementBasedMappedProperty(
                FromParams.Convert(state),
                To,
                MappedPropertyType,
                PropertySegmentConstructors);
        }

        public StringBasedMappedProperty With(
            IAccumulator<StringBasedElement> fromParams = null, 
            string to = null, 
            Type mappedPropertyType = null,
            ConstructorInfo[] constructorArgs = null)
        {
            return new StringBasedMappedProperty(
                fromParams ?? FromParams, 
                to ?? To, 
                mappedPropertyType ?? MappedPropertyType, 
                constructorArgs ?? PropertySegmentConstructors);
        }
    }
    
    class QueryElementBasedMappedProperty : MappedProperty<SelectColumnBasedElement>
    {
        public QueryElementBasedMappedProperty(IAccumulator<SelectColumnBasedElement> fromParams, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs = null)
            : base(fromParams, to, mappedPropertyType, constructorArgs)
        {
        }
    }

    [DebuggerDisplay("{GetDebuggerDisplay()}")]
    class MappedProperty<TElement> : IDebuggerDisplay
    {
        static readonly ConstructorInfo[] EmptyConstructorArgs = new ConstructorInfo[0];

        public readonly Type MappedPropertyType;
        public readonly ConstructorInfo[] PropertySegmentConstructors;
        public readonly string To;
        public readonly IAccumulator<TElement> FromParams;

        public MappedProperty(IAccumulator<TElement> fromParams, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs)
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