using System.Collections.Generic;
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
        public readonly ISqlFragmentBuilder SqlBuilder;

        public BuildMapState(
            string primarySelectTable, 
            ParamBuilder parameters, 
            ParameterExpression queryObject, 
            ParameterExpression argsObject, 
            ISqlStatement wrappedSqlStatement, 
            ISqlFragmentBuilder sqlBuilder)
        {
            Parameters = parameters;
            QueryObject = queryObject;
            ArgsObject = argsObject;
            WrappedSqlStatement = wrappedSqlStatement;
            PrimarySelectTable = primarySelectTable;
            SqlBuilder = sqlBuilder;
        }
    }
}