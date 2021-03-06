using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.Query
{
    /// <summary>
    /// A join to another table
    /// </summary>
    public class Join
    {
        /// <summary>
        /// The type of the join (inner, left, etc)
        /// </summary>
        public readonly JoinType JoinType;

        /// <summary>
        /// The name of the table to join to
        /// </summary>
        public readonly string TableName;
        
        /// <summary>
        /// The expression which describes the join (the ON part)
        /// </summary>
        public readonly (ParameterExpression rootObjectParam, ParameterExpression queryArgs, ParameterExpression joinParam, Expression joinExpression) JoinExpression;
        
        /// <summary>
        /// The parameter to append the joined tables to on the query result class
        /// </summary>
        public readonly (string name, Type type) JoinedTableProperty;

        /// <summary>
        /// Defines whether a join contains a one to one relationship with the primary table
        /// </summary>
        public readonly JoinParent JoinParent;

        public Join(
            JoinType joinType,
            string tableName,
            (ParameterExpression rootObjectParam, ParameterExpression queryArgs, ParameterExpression joinParam, Expression joinExpression) joinExpression,
            (string name, Type type) joinedTableProperty,
            JoinParent joinParent)
        {
            JoinType = joinType;
            TableName = tableName;
            JoinExpression = joinExpression;
            JoinedTableProperty = joinedTableProperty;
            JoinParent = joinParent;
        }
    }
    
    /// <summary>
    /// A type of join
    /// </summary>
    public enum JoinType
    {
        Inner = 1,
        Left
    }

    /// <summary>
    /// Defines whether a join contains a one to one relationship with the primary table
    /// </summary>
    public enum JoinParent
    {
        PrimaryTableOneToOne,
        Other
    }
}
