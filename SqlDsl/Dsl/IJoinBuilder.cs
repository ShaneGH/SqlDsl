using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlDsl.Dsl
{    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IJoinBuilder<TResult, TJoin>
    {
        /// <summary>
        /// Define how a JOIN table joins to other tables in the query
        /// </summary>
        /// <param name="joinExpression">
        /// An expression which describes the [ON] part of JOIN [Table] [ON]
        /// </param>
        IQuery<TResult> On(Expression<Func<TResult, TJoin, bool>> joinExpression);
    }
    
    /// <summary>
    /// Interface to help with query building DSL
    /// </summary>
    public interface IJoinBuilder<TArgs, TResult, TJoin>
    {
        /// <summary>
        /// Define how a JOIN table joins to other tables in the query
        /// </summary>
        /// <param name="joinExpression">
        /// An expression which describes the [ON] part of JOIN [Table] [ON]
        /// </param>
        IQuery<TArgs, TResult> On(Expression<Func<TResult, TJoin, TArgs, bool>> joinExpression);
        
        /// <summary>
        /// Define how a JOIN table joins to other tables in the query
        /// </summary>
        /// <param name="joinExpression">
        /// An expression which describes the [ON] part of JOIN [Table] [ON]
        /// </param>
        IQuery<TArgs, TResult> On(Expression<Func<TResult, TJoin, bool>> joinExpression);
    }
}
