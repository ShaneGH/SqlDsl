using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.Query;
using SqlDsl.SqlBuilders;

namespace SqlDsl.Mapper
{
    class BuildMapState
    {
        public readonly ParamBuilder Parameters;
        public readonly ParameterExpression QueryObject;
        public readonly ParameterExpression ArgsObject;
        public readonly List<(ParameterExpression parameter, IEnumerable<string> property)> ParameterRepresentsProperty = new List<(ParameterExpression, IEnumerable<string>)>();
        public readonly ISqlStatement WrappedSqlStatement;
        public readonly string PrimarySelectTable;
        public readonly ISqlSyntax SqlBuilder;
        public IEnumerable<string> CurrentTable { get; private set; }

        public BuildMapState(
            string primarySelectTable, 
            ParamBuilder parameters, 
            ParameterExpression queryObject, 
            ParameterExpression argsObject, 
            ISqlStatement wrappedSqlStatement, 
            ISqlSyntax sqlBuilder)
        {
            Parameters = parameters;
            QueryObject = queryObject;
            ArgsObject = argsObject;
            WrappedSqlStatement = wrappedSqlStatement;
            PrimarySelectTable = primarySelectTable;
            SqlBuilder = sqlBuilder;
            CurrentTable = PrimarySelectTable.Split('.');
        }

        public IDisposable SwitchContext(ParameterExpression newContext)
        {
            var ctxt = ParameterRepresentsProperty
                .Where(p => p.parameter == newContext)
                .Select(p => p.property)
                .FirstOrDefault();

            if (ctxt == null)
                throw new InvalidOperationException($"Cannot find context for parameter: {newContext}.");

            var currentContext = CurrentTable;
            CurrentTable = ctxt;
            return new GenericDisposable(() => CurrentTable = currentContext);
        }

        class GenericDisposable : IDisposable
        {
            readonly Action Dispose;

            public GenericDisposable(Action dispose)
            {
                Dispose = dispose;
            }

            void IDisposable.Dispose() => Dispose();
        }
    }
}