
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
        public readonly string MappingContext;
        public readonly bool LockMappingContext;

        public StringBasedMappedProperty(
            IAccumulator<StringBasedElement> fromParams, 
            string to, 
            Type mappedPropertyType, 
            string mappingContext, 
            bool lockMappingContext, 
            ConstructorInfo[] constructorArgs = null)
            : base(fromParams, to, mappedPropertyType, constructorArgs)
        {
            MappingContext = mappingContext;
            LockMappingContext = lockMappingContext;
        }
        
        public StringBasedMappedProperty(
            ParameterExpression fromParamRoot, 
            string from, 
            string to, 
            Type mappedPropertyType, 
            string mappingContext, 
            bool lockMappingContext, 
            ConstructorInfo[] constructorArgs = null, 
            string aggregatedToTable = null)

            : this (new Accumulator<StringBasedElement>(
                new Accumulator<StringBasedElement, BinarySqlOperator>(
                    new StringBasedElement(fromParamRoot, from, aggregatedToTable))), to, mappedPropertyType, mappingContext, lockMappingContext, constructorArgs)
        {
        }

        public QueryElementBasedMappedProperty Convert(BuildMapState state)
        {
            var mappingContext = state.WrappedSqlStatement.Tables[MappingContext];

            return new QueryElementBasedMappedProperty(
                FromParams.Convert(state),
                To,
                MappedPropertyType,
                mappingContext,
                PropertySegmentConstructors);
        }

        private StringBasedMappedProperty With(
            string mappingContext,
            bool lockMappingContext,
            IAccumulator<StringBasedElement> fromParams = null, 
            string to = null, 
            Type mappedPropertyType = null,
            ConstructorInfo[] constructorArgs = null)
        {
            return new StringBasedMappedProperty(
                fromParams ?? FromParams, 
                to ?? To, 
                mappedPropertyType ?? MappedPropertyType,
                LockMappingContext ? MappingContext : mappingContext,
                LockMappingContext || lockMappingContext, 
                constructorArgs ?? PropertySegmentConstructors);
        }

        /// <summary>
        /// Create another mapped property with the option to lock
        /// </summary>
        public StringBasedMappedProperty WithLock(
            string mappingContext,
            bool lockMappingContext = true,
            IAccumulator<StringBasedElement> fromParams = null, 
            string to = null, 
            Type mappedPropertyType = null,
            ConstructorInfo[] constructorArgs = null)
        {
            return With(mappingContext, lockMappingContext, fromParams, to, mappedPropertyType, constructorArgs);
        }

        /// <summary>
        /// Create another mapped property with the the same lock as this object
        /// </summary>
        public StringBasedMappedProperty With(
            string mappingContext,
            IAccumulator<StringBasedElement> fromParams = null, 
            string to = null, 
            Type mappedPropertyType = null,
            ConstructorInfo[] constructorArgs = null)
        {
            return With(mappingContext, false, fromParams, to, mappedPropertyType, constructorArgs);
        }
    }
    
    class QueryElementBasedMappedProperty : MappedProperty<SelectColumnBasedElement>
    {
        public readonly IQueryTable MappingContext;

        public QueryElementBasedMappedProperty(IAccumulator<SelectColumnBasedElement> fromParams, string to, Type mappedPropertyType, IQueryTable mappingContext, ConstructorInfo[] constructorArgs = null)
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