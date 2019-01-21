using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using SqlDsl.Dsl;
using SqlDsl.Utils;

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
            throw new InvalidOperationException(CodingConstants.Messages.MethodIsForExpression);
        }

        /// <summary>
        /// When used in a query expression, allows represents a sql IN expression
        /// </summary>
        public static bool In<T>(this T value, IEnumerable<T> values)
        {
            throw new InvalidOperationException(CodingConstants.Messages.MethodIsForExpression);
        }

        /// <summary>
        /// When used in a query expression, represents the row number of the primary table. Can be used for paging
        /// </summary>
        public static int RowNumber()
        {
            throw new InvalidOperationException(CodingConstants.Messages.MethodIsForExpression);
        }

        public static class Case
        {
            /// <summary>
            /// Begin a sql CASE statement
            /// </summary>
            public static ICase When(bool condition) => throw new InvalidOperationException(CodingConstants.Messages.MethodIsForExpression);

            /// <summary>
            /// Begin a sql CASE statement which has a subject
            /// </summary>
            public static ISimpleCase<TSubject> Simple<TSubject>(TSubject subject) => throw new InvalidOperationException(CodingConstants.Messages.MethodIsForExpression);
        }
    }

    public class QueryBuilder { }
}
