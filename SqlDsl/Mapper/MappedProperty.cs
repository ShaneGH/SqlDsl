
using System;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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

        public string GetDebugView(BuildMapState state) => new[]
        {
            $"Type: {MappedPropertyType}",
            $"CArgs: {PropertySegmentConstructors.JoinString(", ")}",
            $"From: {TryGetFromString(state)}",
            $"To: {To}"
        }
        .RemoveNullOrEmpty()
        .JoinString("\n");

        /// <summary>
        /// Debug only
        /// </summary>
        string TryGetFromString(BuildMapState state)
        {
            try
            {
                return FromParams.BuildFromString(state, new DebugOnlySqlFragmentBuilder());
            }
            catch (Exception e)
            {
                return $"Cannot build from string: {e}";
            }
        }

        class DebugOnlySqlFragmentBuilder : SqlBuilders.SqlSyntaxBase
        {
            public override (string setupSql, string sql) GetSelectTableSqlWithRowId(string tableName, string rowIdAlias)
            {
                throw new NotImplementedException();
            }

            public override string WrapAlias(string alias) => $"[{alias}]";

            public override string WrapColumn(string column) => $"[{column}]";

            public override string WrapTable(string table) => $"[{table}]";
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