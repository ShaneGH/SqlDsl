
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    class StringBasedMappedProperty : MappedProperty<StringBasedElement>
    {
        public StringBasedMappedProperty(IAccumulator<StringBasedElement> fromParams, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs = null)
            : base(fromParams, to, mappedPropertyType, constructorArgs)
        {
        }
        
        public StringBasedMappedProperty(ParameterExpression fromParamRoot, string from, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs = null)
            : this (new Accumulator<StringBasedElement>(
                new Accumulator<StringBasedElement, CombinationType>(
                    new StringBasedElement(fromParamRoot, from, null, null))), to, mappedPropertyType, constructorArgs)
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
    
    class QueryElementBasedMappedProperty : MappedProperty<ColumnBasedElement>
    {
        public QueryElementBasedMappedProperty(IAccumulator<ColumnBasedElement> fromParams, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs = null)
            : base(fromParams, to, mappedPropertyType, constructorArgs)
        {
        }
    }

    class MappedProperty<TElement>
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
    }
}