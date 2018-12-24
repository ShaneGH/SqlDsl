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
    /// Extra values on a statement which describe the mapping of another query
    /// </summary>
    class MappingProperties : IMappingProperties
    {
        /// <summary>
        /// The other (mapped from) query
        /// </summary>
        public ISqlStatement InnerStatement => InnerQuery.InnerStatement;
        
        /// <summary>
        /// The inner query
        /// </summary>
        readonly ISqlStatementPartValues InnerQuery;
        
        /// <summary>
        /// A list of column name prefixes which are bound to a specific table, along with an index to reference that table
        /// </summary>
        public IEnumerable<(string columnGroupPrefix, int rowNumberColumnIndex)> ColumnGroupRowNumberColumIndex => GetColumnGroupRowNumberColumIndex();

        public MappingProperties(ISqlStatementPartValues mappedStatement)
        {
            InnerQuery = mappedStatement ?? throw new ArgumentNullException(nameof(mappedStatement));
            if (InnerQuery.InnerStatement == null)
                throw new InvalidOperationException("Invalid mapped statement.");
        }

        /// <summary>
        /// Get a list of column name prefixes which are bound to a specific table, along with an index to reference that table
        /// </summary>
        IEnumerable<(string columnGroupPrefix, int rowNumberColumnIndex)> GetColumnGroupRowNumberColumIndex()
        {
            // if this function is not returning the correct data or RowIdColumnNumbers of ObjectPropertyGraph are invalid
            // check mapped tables in QueryMapper.BuildMapForSelect(...)

            return InnerQuery.RowIdsForMappedProperties
                .Select(x => (x.resultClassProperty, InnerStatement.SelectColumns[x.rowIdColumnName].RowNumberColumnIndex));
        }
    }
}