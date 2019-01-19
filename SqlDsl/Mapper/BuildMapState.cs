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
        public (ParameterExpression tableParam, IEnumerable<string> propertyName) MappingContext { get; private set; }
        public readonly bool UseColumnAliases;

        public BuildMapState(
            string primarySelectTableAlias, 
            ParamBuilder parameters, 
            ParameterExpression queryObject, 
            ParameterExpression argsObject, 
            ISqlStatement wrappedSqlStatement, 
            ISqlSyntax sqlBuilder,
            bool useColumnAliases)
        {
            Parameters = parameters;
            QueryObject = queryObject;
            ArgsObject = argsObject;
            WrappedSqlStatement = wrappedSqlStatement;
            PrimarySelectTableAlias = primarySelectTableAlias;
            SqlBuilder = sqlBuilder;
            MappingContext = (queryObject, PrimarySelectTableAlias.Split('.'));
            UseColumnAliases = useColumnAliases;
        }

        public IDisposable SwitchContext(ParameterExpression newContext)
        {
            var ctxt = ParameterRepresentsProperty
                .Where(p => p.parameter == newContext)
                .AsNullable()
                .FirstOrDefault();

            if (ctxt == null)
                throw new InvalidOperationException($"Cannot find context for parameter: {newContext}.");

            var currentContext = MappingContext;
            MappingContext = ctxt.Value;
            return ReusableGenericDisposable.Build(() => MappingContext = currentContext);
        }
    }
}