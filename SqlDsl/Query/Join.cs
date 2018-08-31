using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.Query
{
    public class Join
    {
        public readonly JoinType JoinType;
        public readonly string TableName;
        public readonly (ParameterExpression rootObjectParam, ParameterExpression joinParam, Expression joinExpression) JoinExpression;
        public readonly (string name, Type type) JoinResult;

        public Join(
            JoinType joinType,
            string tableName,
            (ParameterExpression rootObjectParam, ParameterExpression joinParam, Expression joinExpression) joinExpression,
            (string name, Type type) joinResult)
        {
            JoinType = joinType;
            TableName = tableName;
            JoinExpression = joinExpression;
            JoinResult = joinResult;
        }
    }
    
    public enum JoinType
    {
        Inner = 1,
        Left
    }
}
