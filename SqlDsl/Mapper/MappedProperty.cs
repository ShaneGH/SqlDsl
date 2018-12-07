
using System;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.Mapper
{
    class MappedProperty
    {
        static readonly ConstructorInfo[] EmptyConstructorArgs = new ConstructorInfo[0];

        public readonly Type MappedPropertyType;
        public readonly ConstructorInfo[] PropertySegmentConstructors;
        public readonly string To;
        public readonly Accumulator FromParams;

        public MappedProperty(Accumulator fromParams, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs = null)
        {
            To = to;
            FromParams = fromParams;
            MappedPropertyType = mappedPropertyType;
            PropertySegmentConstructors = constructorArgs ?? EmptyConstructorArgs;
        }
        
        public MappedProperty(ParameterExpression fromParamRoot, string from, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs = null)
            : this (new Accumulator(fromParamRoot, from), to, mappedPropertyType, constructorArgs)
        {
        }
    }
}