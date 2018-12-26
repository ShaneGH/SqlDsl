
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    class MappedProperty
    {
        static readonly ConstructorInfo[] EmptyConstructorArgs = new ConstructorInfo[0];

        public readonly Type MappedPropertyType;
        public readonly ConstructorInfo[] PropertySegmentConstructors;
        public readonly string To;
        public readonly IAccumulator<Element> FromParams;

        public MappedProperty(IAccumulator<Element> fromParams, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs = null)
        {
            To = to;
            FromParams = fromParams;
            MappedPropertyType = mappedPropertyType;
            PropertySegmentConstructors = constructorArgs ?? EmptyConstructorArgs;
        }
        
        public MappedProperty(ParameterExpression fromParamRoot, string from, string to, Type mappedPropertyType, ConstructorInfo[] constructorArgs = null)
            : this (new Accumulator<Element>(
                new Accumulator<Element, CombinationType>(
                    new Element(fromParamRoot, from, null, null))), to, mappedPropertyType, constructorArgs)
        {
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
                return FromParams.BuildFromString(state, new SqlFragmentBuilder());
            }
            catch (Exception e)
            {
                return $"Cannot build from string: {e}";
            }
        }

        class SqlFragmentBuilder : SqlBuilders.SqlFragmentBuilderBase
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
}