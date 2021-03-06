using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using SqlDsl.Dsl;
using SqlDsl.Mapper;
using SqlDsl.Utils;

namespace SqlDsl
{
    /// <summary>
    /// Utils to build and execute Sql queries
    /// </summary>
    public static class Sql
    {
        const string InvalidMessageString = "The \"{0}\" method is not valid in this context. \nPlease ensure that you are only using this method on table values within a sql query. \nDo not use on query constants or query arguments.";

        /// <summary>
        /// Create a sql query
        /// </summary>
        public static readonly QueryBuilder Query = new QueryBuilder();

        /// <summary>
        /// When used in a query expression, allows the comparison of a single object property to a property of an object in a list
        /// </summary>
        public static T One<T>(this IEnumerable<T> values)
        {
            throw new SqlBuilderException(string.Format(InvalidMessageString, "One"));
        }

        /// <summary>
        /// When used in a query expression, allows represents a sql IN expression
        /// </summary>
        public static bool In<T>(this T value, IEnumerable<T> values)
        {
            throw new SqlBuilderException(string.Format(InvalidMessageString, "In"));
        }

        /// <summary>
        /// When used in a query expression, represents the row number used to page and sort
        /// records. For internal use only
        /// </summary>
        internal static int OrderByRowNumber()
        {
            throw new SqlBuilderException(string.Format(InvalidMessageString, "OrderByRowNumber"));
        }

        public static class Case
        {
            /// <summary>
            /// Begin a sql CASE statement
            /// </summary>
            public static ICase When(bool condition) => throw new SqlBuilderException(string.Format(InvalidMessageString, "Case.When"));

            /// <summary>
            /// Begin a sql CASE statement which has a subject
            /// </summary>
            public static ISimpleCase<TSubject> Simple<TSubject>(TSubject subject) => throw new SqlBuilderException(string.Format(InvalidMessageString, "Case.Simple"));
        }
    }

    public class QueryBuilder { }
}
