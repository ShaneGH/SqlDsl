using SqlDsl.Query;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    /// <summary>
    /// A collection of items build using the query dsl
    /// </summary>
    public class SqlBuilderItems // TODO: bad name
    {
        /// <summary>
        /// The type of the sql query
        /// </summary>
        public readonly QueryType QueryType;

        /// <summary>
        /// A builder to build the actual sql
        /// </summary>
        public readonly ISqlBuilder Builder;
        
        /// <summary>
        /// If QueryType == Complex, represents a statement which provides metadata
        /// Otherwise the value of this property should be ignored
        /// </summary>
        public readonly ISqlStatement Statement;

        /// <summary>
        /// If QueryType == Simple, will specify the column index of the simple value column
        /// Otherwise the value of this property should be ignored
        /// </summary>
        public readonly int SimpleValueColumnIndex;

        /// <summary>
        /// If QueryType == Simple, will specify the row number column index for the SimpleValueColumn
        /// Otherwise the value of this property should be ignored
        /// </summary>
        public readonly int SimpleValueRowNumberColumnIndex;

        public SqlBuilderItems(ISqlBuilder builder, ISqlStatement statement)
        {
            Builder = builder ?? throw new ArgumentNullException(nameof(builder));
            Statement = statement ?? throw new ArgumentNullException(nameof(statement));
            QueryType = QueryType.Complex;
        }

        public SqlBuilderItems(ISqlBuilder builder, int simpleValueColumnIndex, int simpleValueRowNumberColumnIndex)
        {
            Builder = builder ?? throw new ArgumentNullException(nameof(builder));
            SimpleValueColumnIndex = simpleValueColumnIndex;
            SimpleValueRowNumberColumnIndex = simpleValueRowNumberColumnIndex;
        }
    }

    public enum QueryType
    {
        /// <summary>
        /// A query which parses to complex objects
        /// </summary>
        Complex = 1,
        
        /// <summary>
        /// A query which parses to simple objects (e.g. string, int etc...)
        /// </summary>
        Simple
    }
}
