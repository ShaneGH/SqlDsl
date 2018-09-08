using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace SqlDsl
{
    /// <summary>
    /// Utils to build and execute Sql queries
    /// </summary>
    public static class Sql
    {
        /// <summary>
        /// Create a sql query
        /// </summary>
        public static readonly QueryBuilder Query = new QueryBuilder();

        /// <summary>
        /// When used in a query expression, allows the comparison of a single object property to a property of an object in a list
        /// </summary>
        public static T One<T>(this IEnumerable<T> values)
        {
            throw new InvalidOperationException("This method is not meant to be called. It should be used in expressions only");
        }

        // TODO: is this needed in conjunction with .Joined()
        // /// <summary>
        // /// When used in a query expression, allows an object to be converted into an enumerable
        // /// </summary>
        // public static IEnumerable<T> Many<T>(this T value)
        // {
        //     throw new InvalidOperationException("This method is not meant to be called. It should be used in expressions only");
        // }

        /// <summary>
        /// When used in a query map expression, gets a reference to a table which this table joins to
        /// </summary>
        /// <param name="item">The item which represents the joined table</param>
        /// <param name="joinedItems">An expresion which starts at the query object and points to the table which is joined to
        /// e.g. Map(queryObject => ... item.Joined(() => queryObject.JoinedTo) ...) . 
        /// use the "One()" function if the result of the the Joined(...) call is an enumerable</param>
        public static U Joined<T, U>(this T item, Expression<Func<U>> joinedItems)
        {
            throw new InvalidOperationException("This method is not meant to be called. It should be used in expressions only");
        }
    }

    public class QueryBuilder { }
}
