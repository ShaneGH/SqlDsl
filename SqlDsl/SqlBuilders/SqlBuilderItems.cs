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
        /// A builder to build the actual sql
        /// </summary>
        public readonly ISqlBuilder Builder;
        
        /// <summary>
        /// If QueryType == Complex, represents a statement which provides metadata
        /// Otherwise the value of this property should be ignored
        /// </summary>
        public readonly ISqlStatement Statement;

        public SqlBuilderItems(ISqlBuilder builder, ISqlStatement statement)
        {
            Builder = builder ?? throw new ArgumentNullException(nameof(builder));
            Statement = statement ?? throw new ArgumentNullException(nameof(statement));
        }
    }
}
