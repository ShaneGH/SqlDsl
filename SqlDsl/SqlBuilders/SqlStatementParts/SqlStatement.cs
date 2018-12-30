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
    // public class SqlStatement2 : ISqlStatement
    // {
    //     public string UniqueAlias { get; }

    //     public IQueryTables Tables { get; }

    //     public IMappingProperties MappingProperties { get; }

    //     public ISelectColumns SelectColumns { get; }

    //     public IQueryTable GetTableForColum(string columnAlias) => TryGetTableForColum(columnAlias) ?? 
    //         throw new InvalidOperationException($"There is no table for column alias: {columnAlias}.");

    //     public IQueryTable TryGetTableForColum(string columnAlias) => Tables.TryGetTable(SelectColumns
    //         .TryGetColumn(columnAlias)
    //         ?.Alias);

    //     public SqlStatement2(string uniqueAlias, IEnumerable<IQueryTable> tables, IEnumerable<ISelectColumn> columns, ISqlStatement innerStatement, IEnumerable<(string rowIdColumnName, string resultClassProperty)> rowIdsForMappedProperties)
    //     {
    //         UniqueAlias = uniqueAlias;
    //         Tables = new QueryTables(tables);
    //         SelectColumns = new SelectCols(columns);
    //         MappingProperties = new MappingProps(innerStatement, rowIdsForMappedProperties);
    //     }

    //     class SelectCols : ISelectColumns
    //     {
    //         public ISelectColumn this[string alias] => TryGetColumn(alias) ?? 
    //             throw new InvalidOperationException($"There is no column with alias: {alias}.");

    //         readonly IEnumerable<ISelectColumn> Columns;

    //         public SelectCols(IEnumerable<ISelectColumn> columns)
    //         {
    //             Columns = columns ?? throw new ArgumentNullException(nameof(columns));
    //         }

    //         public IEnumerator<ISelectColumn> GetEnumerator() => Columns.GetEnumerator();

    //         public ISelectColumn TryGetColumn(string alias) => Columns.FirstOrDefault(t => t.Alias == alias);

    //         IEnumerator IEnumerable.GetEnumerator() => (Columns as IEnumerable).GetEnumerator();
    //     }

    //     class QueryTables : IQueryTables
    //     {
    //         public IQueryTable this[string alias] => TryGetTable(alias) ?? 
    //             throw new InvalidOperationException($"There is no table with alias: {alias}.");

    //         readonly IEnumerable<IQueryTable> Tables;

    //         public QueryTables(IEnumerable<IQueryTable> tables)
    //         {
    //             Tables = tables ?? throw new ArgumentNullException(nameof(tables));
    //         }

    //         public IEnumerator<IQueryTable> GetEnumerator() => Tables.GetEnumerator();

    //         public IQueryTable TryGetTable(string alias) => Tables.FirstOrDefault(t => t.Alias == alias);

    //         IEnumerator IEnumerable.GetEnumerator() => (Tables as IEnumerable).GetEnumerator();
    //     }

    //     /// <summary>
    //     /// Extra values on a statement which describe the mapping of another query
    //     /// </summary>
    //     class MappingProps : IMappingProperties
    //     {
    //         /// <summary>
    //         /// The other (mapped from) query
    //         /// </summary>
    //         public ISqlStatement InnerStatement { get; }
            
    //         /// <summary>
    //         /// The inner query
    //         /// </summary>
    //         readonly IEnumerable<(string rowIdColumnName, string resultClassProperty)> RowIdsForMappedProperties;
            
    //         /// <summary>
    //         /// A list of column name prefixes which are bound to a specific table, along with an index to reference that table
    //         /// </summary>
    //         public IEnumerable<(string columnGroupPrefix, ISelectColumn rowNumberColumn)> ColumnGroupRowNumberColumIndex => GetColumnGroupRowNumberColumIndex();

    //         public MappingProps(ISqlStatement innerStatement, IEnumerable<(string rowIdColumnName, string resultClassProperty)> rowIdsForMappedProperties)
    //         {
    //             InnerStatement = innerStatement ?? throw new ArgumentNullException(nameof(innerStatement));
    //             RowIdsForMappedProperties = rowIdsForMappedProperties ?? throw new ArgumentNullException(nameof(rowIdsForMappedProperties));
    //         }

    //         /// <summary>
    //         /// Get a list of column name prefixes which are bound to a specific table, along with an index to reference that table
    //         /// </summary>
    //         IEnumerable<(string columnGroupPrefix, ISelectColumn rowNumberColumn)> GetColumnGroupRowNumberColumIndex()
    //         {
    //             // if this function is not returning the correct data or RowIdColumnNumbers of ObjectPropertyGraph are invalid
    //             // check mapped tables in QueryMapper.BuildMapForSelect(...)

    //             return RowIdsForMappedProperties
    //                 .Select(x => (x.resultClassProperty, InnerStatement.SelectColumns[x.rowIdColumnName].Table.RowNumberColumn));
    //         }
    //     }
    // }

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
            Tables = new QueryTables(builder, this);
            SelectColumns = new SelectColumns(builder, Tables);
            MappingProperties = builder.InnerStatement == null ?
                null :
                new MappingProperties(builder);
        }

        /// <inheritdoc />
        public IQueryTable TryGetTableForColum(string columnAlias)
        {
            var col = SelectColumns.TryGetColumn(columnAlias);
            if (col == null) return null;

            return col.Table;
        }

        /// <inheritdoc />
        public IQueryTable GetTableForColum(string columnAlias)
        {
            var col = SelectColumns[columnAlias];
            return col.Table;
        }
    }
}