using System;
using System.Collections.Generic;

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
        public static T One<T>(IEnumerable<T> values)
        {
            throw new InvalidOperationException("This method is not meant to be called. It should be used in expressions only");
        }
    }

    public class QueryBuilder { }
}
