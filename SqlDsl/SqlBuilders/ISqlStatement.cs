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
    public interface ISqlStatement : ISqlSelectStatement
    {
        /// <summary>
        /// The tables involved in the statement
        /// </summary>
        IQueryTables Tables { get; }
    }
    
    /// <summary>
    /// Describes a sql statement
    /// </summary>
    public interface ISqlSelectStatement
    {
        /// <summary>
        /// A list of property names on the mapper output along with the row number which index them.
        /// </summary>
        IEnumerable<(string mappedPropertyName, ISelectColumn rowNumberColumn)> MappedPropertiesToRowNumbers { get; }

        /// <summary>
        /// The columns in the SELECT part of the query
        /// </summary>
        ISelectColumns SelectColumns { get; }
    }

    /// <summary>
    /// A list of tables in the query.false (FROM and JOIN)
    /// </summary>
    public interface IQueryTables : IEnumerable<IQueryTable>
    {        
        /// <summary>
        /// Get a table based on it's alias
        /// </summary>
        IQueryTable this[string alias] { get; }

        /// <summary>
        /// Get a table based on its alias
        /// </summary>
        IQueryTable TryGetTable(string alias);
    }

    /// <summary>
    /// A list of columns in the SELECT statement
    /// </summary>
    public interface ISelectColumns : IEnumerable<ISelectColumn>
    {        
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
    /// A table within a query
    /// </summary>
    public interface IQueryTable
    {
        /// <summary>
        /// The table alias
        /// </summary>
        string Alias { get; }
        
        /// <summary>
        /// If this table is in a join, will be the tables that it is joined on.
        /// Otherwise it will be empty
        /// </summary>
        IQueryTable[] JoinedFrom { get; }

        /// <summary>
        /// The primary key of this table
        /// </summary>
        ISelectColumn RowNumberColumn { get; }

        /// <summary>
        /// The type of the join, or null if this table is the FROM table
        /// </summary>
        JoinType? JoinType { get; }
    }
    
    /// <summary>
    /// A column in the SELECT statement
    /// </summary>
    public interface ISelectColumn
    {        
        /// <summary>
        /// The row which acts as a primary key for this column
        /// </summary>
        ISelectColumn RowNumberColumn { get; }
        
        /// <summary>
        /// The alias of the column
        /// </summary>
        string Alias { get; }
        
        /// <summary>
        /// If not null, this column is a row number for the given table
        /// </summary>
        IQueryTable IsRowNumberForTable { get; }    // TODO: sub interface for this property
        
        /// <summary>
        /// Refers to the table which provided context for the mapping of this property
        /// </summary>
        IQueryTable MappingContext { get; }
        
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
