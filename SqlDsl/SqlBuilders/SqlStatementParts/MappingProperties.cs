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
        public ISqlStatement InnerStatement { get; }
        
        readonly IEnumerable<(string rowIdColumnName, string resultClassProperty)> RowIdsForMappedProperties;
        
        /// <summary>
        /// A list of column name prefixes which are bound to a specific table, along with an index to reference that table
        /// </summary>
        public IEnumerable<(string columnGroupPrefix, ISelectColumn rowNumberColumn)> ColumnGroupRowNumberColumIndex => GetColumnGroupRowNumberColumIndex();

        public MappingProperties(ISqlStatementPartValues mappedStatement)
            : this(mappedStatement?.InnerStatement, mappedStatement?.RowIdsForMappedProperties)
        {
        }

        public MappingProperties(ISqlStatement innerStatement, IEnumerable<(string rowIdColumnName, string resultClassProperty)> rowIdsForMappedProperties)
        {
            InnerStatement = innerStatement ?? throw new ArgumentNullException(nameof(innerStatement));
            RowIdsForMappedProperties = rowIdsForMappedProperties ?? throw new ArgumentNullException(nameof(rowIdsForMappedProperties));
        }

        /// <summary>
        /// Get a list of column name prefixes which are bound to a specific table, along with an index to reference that table
        /// </summary>
        IEnumerable<(string columnGroupPrefix, ISelectColumn rowNumberColumn)> GetColumnGroupRowNumberColumIndex()
        {
            // if this function is not returning the correct data or RowIdColumnNumbers of ObjectPropertyGraph are invalid
            // check mapped tables in QueryMapper.BuildMapForSelect(...)

            return RowIdsForMappedProperties
                .Select(x => (x.resultClassProperty, InnerStatement.SelectColumns[x.rowIdColumnName].Table.RowNumberColumn));
        }
    }
}