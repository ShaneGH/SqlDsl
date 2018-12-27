using SqlDsl.Query;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    /// <summary>
    /// Describes a sql statement
    /// </summary>
    public interface ISqlStatement
    {
        /// <summary>
        /// A unique alias for this statement. Uniqueness must be guaranteed within a single statement only
        /// </summary>
        string UniqueAlias { get; }

        /// <summary>
        /// The tables involved in the statement
        /// </summary>
        IQueryTables Tables { get; }

        /// <summary>
        /// If the statement is a mapped statement, i.e. it has an inner wrapped query, this property will show the details of this.
        /// Otherwise It will be null.
        /// </summary>
        IMappingProperties MappingProperties { get; }

        /// <summary>
        /// The columns in the SELECT part of the query
        /// </summary>
        ISelectColumns SelectColumns { get; }

        /// <summary>
        /// Get the table which this column belongs to or null
        /// </summary>
        IQueryTable TryGetTableForColum(string columnAlias);

        /// <summary>
        /// Get the table which this column belongs to
        /// </summary>
        IQueryTable GetTableForColum(string columnAlias);
    }

    /// <summary>
    /// A list of tables in the query.false (FROM and JOIN)
    /// </summary>
    public interface IQueryTables : IEnumerable<IQueryTable>
    {
        /// <summary>
        /// Get a table based on the index of its row number column
        /// </summary>
        IQueryTable this[int rowNumberColumnIndex] { get; }
        
        /// <summary>
        /// Get a table based on it's alias
        /// </summary>
        IQueryTable this[string alias] { get; }

        /// <summary>
        /// Get a table based on the index of its row number column
        /// </summary>
        IQueryTable TryGetTable(int rowNumberColumnIndex);
    }

    /// <summary>
    /// A list of columns in the SELECT statement
    /// </summary>
    public interface ISelectColumns : IEnumerable<ISelectColumn>
    {
        /// <summary>
        /// Get a column based on it's index in the select statement
        /// </summary>
        ISelectColumn this[int index] { get; }
        
        /// <summary>
        /// Get a column based on it's alias
        /// </summary>
        ISelectColumn this[string alias] { get; }

        /// <summary>
        /// Get a column based on it's alias, or null if it doesn't exist
        /// </summary>
        ISelectColumn TryGetColumn(string alias);
    }

    /// <summary>
    /// Extra values on a statement which describe the mapping of another query
    /// </summary>
    public interface IMappingProperties
    {
        /// <summary>
        /// The other (mapped from) query
        /// </summary>
        ISqlStatement InnerStatement { get; }
        
        /// <summary>
        /// A list of column name prefixes which are bound to a specific table, along with an index to reference that table
        /// </summary>
        IEnumerable<(string columnGroupPrefix, int rowNumberColumnIndex)> ColumnGroupRowNumberColumIndex { get; }
    }

    /// <summary>
    /// A table within a query
    /// </summary>
    public interface IQueryTable
    {
        /// <summary>
        /// The table alias
        /// </summary>
        string Alias { get; }
        
        /// <summary>
        /// The index of the column which provides row numbers for this table
        /// </summary>
        int RowNumberColumnIndex { get; }
        
        /// <summary>
        /// If this table is in a join, will be the table that it is joined on.
        /// Otherwise it will be null
        /// </summary>
        IQueryTable JoinedFrom { get; }
    }
    
    /// <summary>
    /// A column in the SELECT statement
    /// </summary>
    public interface ISelectColumn
    {
        /// <summary>
        /// The actual columns and tables that this SELECT column represents
        /// </summary>
        (string table, string column, string aggregatedToTable)[] ReferencesColumns { get; }
        
        /// <summary>
        /// The alias of the column
        /// </summary>
        string Alias { get; }
        
        /// <summary>
        /// The index of the row number column for the table which exposes this column
        /// </summary>
        int RowNumberColumnIndex { get; }
        
        /// <summary>
        /// If true, this column is a row number
        /// </summary>
        bool IsRowNumber { get; }

        /// <summary>
        /// The data type of the cell
        /// </summary>
        Type DataType { get; }
        
         /// <summary>
        /// The constructors that this column will be passed into. A column can have more than one constructor if constructors are nested
        /// </summary>
        ConstructorInfo[] ArgConstructors { get; }
    }
}
