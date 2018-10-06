using SqlDsl.Query;
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
        /// Generate sql. Setup sql will be executd before querySql, but in the same sql script
        /// </summary>
        (string querySetupSql, string querySql) ToSqlString();
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
    /// A list of tables in the query.false (FROM and JOIN)
    /// </summary>
    public interface IQueryTables : IEnumerable<IQueryTable>
    {
        /// <summary>
        /// Get a table based on the index of its row number column
        /// </summary>
        IQueryTable this[int rowNumberColumnIndex] { get; }
        
        /// <summary>
        /// Get a table based on it's index
        /// </summary>
        IQueryTable this[string alias] { get; }
    }
    
    /// <summary>
    /// A column in the SELECT statement
    /// </summary>
    public interface ISelectColumn
    {
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
}
