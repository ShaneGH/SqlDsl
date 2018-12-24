using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders.SqlStatementParts
{
    /// <summary>
    /// Describes a sql statement
    /// </summary>
    public class SqlStatement : ISqlStatement
    {
        /// <summary>
        /// A unique alias for this statement. Uniqueness must be guaranteed within a single statement only
        /// </summary>
        public string UniqueAlias { get; }

        /// <summary>
        /// The tables involved in the statement
        /// </summary>
        public IQueryTables Tables { get; }

        /// <summary>
        /// The columns in the SELECT part of the query
        /// </summary>
        public ISelectColumns SelectColumns { get; }

        /// <summary>
        /// If the statement is a mapped statement, i.e. it has an inner wrapped query, this property will show the details of this.
        /// Otherwise It will be null.
        /// </summary>
        public IMappingProperties MappingProperties { get; }

        public SqlStatement(ISqlStatementPartValues builder)
        {
            UniqueAlias = builder.UniqueAlias;
            Tables = new QueryTables(builder);
            SelectColumns = new SelectColumns(builder, Tables);
            MappingProperties = builder.InnerStatement == null ?
                null :
                new MappingProperties(builder);
        }
    }
}