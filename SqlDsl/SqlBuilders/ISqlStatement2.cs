using SqlDsl.Query;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    public interface IQueryTable
    {
        string Alias { get; }
        int RowNumberColumnIndex { get; }
        IQueryTable JoinedFrom { get; }
        //IEnumerable<IQueryTable> IsWrapperFor { get; }
    }

    public interface IQueryTables : IEnumerable<IQueryTable>
    {
        IQueryTable this[int rowNumberColumnIndex] { get; }
        IQueryTable this[string alias] { get; }
    }
    
    public interface ISelectColumn
    {
        string Alias { get; }
        
        int RowNumberColumnIndex { get; }
        
        bool IsRowNumber { get; }
    }

    public interface ISelectColumns : IEnumerable<ISelectColumn>
    {
        ISelectColumn this[int rowNumberColumnIndex] { get; }
        ISelectColumn this[string alias] { get; }
    }

    public interface ISqlStatement2
    {
        /// <summary>
        /// A unique alias for this statement. Uniqueness must be guaranteed within a single statement only
        /// </summary>
        string UniqueAlias { get; }

        IQueryTables Tables { get; }

        IMappingProperties MappingProperties { get; }

        ISelectColumns SelectColumns { get; }

        /// <summary>
        /// Generate sql. Setup sql will be executd before querySql, but in the same sql script
        /// </summary>
        (string querySetupSql, string querySql) ToSqlString();
    }

    public interface IMappingProperties
    {
        ISqlStatement2 InnerStatement { get; }
        IEnumerable<(string columnGroupPrefix, int rowNumberColumnIndex)> ColumnGroupRowNumberColumIndex { get; }
    }
}
