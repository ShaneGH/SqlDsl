using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.Query;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    class BuildMapState
    {
        public readonly ParamBuilder Parameters;
        public readonly ParameterExpression QueryObject;
        public readonly ParameterExpression ArgsObject;
        public readonly List<(ParameterExpression parameter, IEnumerable<string> property)> ParameterRepresentsProperty = new List<(ParameterExpression, IEnumerable<string>)>();
        public readonly ISqlStatement WrappedSqlStatement;
        public readonly string PrimarySelectTableAlias;
        public readonly ISqlSyntax SqlBuilder;
        public (ParameterExpression tableParam, string propertyName) MappingContext { get; private set; }
        public readonly bool UseColumnAliases;
        public readonly MappingPurpose MappingPurpose;

        public BuildMapState(
            string primarySelectTableAlias, 
            ParamBuilder parameters, 
            ParameterExpression queryObject, 
            ParameterExpression argsObject, 
            ISqlStatement wrappedSqlStatement, 
            ISqlSyntax sqlBuilder,
            bool useColumnAliases,
            MappingPurpose mappingPurpose)
        {
            Parameters = parameters;
            QueryObject = queryObject;
            ArgsObject = argsObject;
            WrappedSqlStatement = wrappedSqlStatement;
            PrimarySelectTableAlias = primarySelectTableAlias;
            SqlBuilder = sqlBuilder;
            MappingContext = (queryObject, PrimarySelectTableAlias);
            UseColumnAliases = useColumnAliases;
            MappingPurpose = mappingPurpose;
        }

        public IDisposable SwitchContext(ParameterExpression newContext)
        {
            return SwitchContext(newContext, DefaultOnContextNotFound);
        }

        public IDisposable SwitchContext(ParameterExpression newContext, Func<ParameterExpression, IDisposable> onContextNotFound)
        {
            var ctxt = ParameterRepresentsProperty
                .Where(p => p.parameter == newContext)
                .AsNullable()
                .FirstOrDefault();

            if (ctxt == null)
                return onContextNotFound(newContext);

            var currentContext = MappingContext;
            MappingContext = (ctxt.Value.parameter, ctxt.Value.property.JoinString("."));
            return ReusableGenericDisposable.Build(() => MappingContext = currentContext);
        }

        static readonly Func<ParameterExpression, IDisposable> DefaultOnContextNotFound = newContext => 
            throw new InvalidOperationException($"Cannot find context for parameter: {newContext}.");
    }
}