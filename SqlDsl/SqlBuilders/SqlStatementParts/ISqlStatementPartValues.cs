using System;
using System.Collections.Generic;
using System.Reflection;

namespace SqlDsl.SqlBuilders.SqlStatementParts
{
    public interface ISqlStatementPartValues
    {
        string UniqueAlias { get; }
        string PrimaryTableAlias { get; }
        IEnumerable<SqlStatementPartJoin> JoinTables { get; }
        ISqlStatement InnerStatement { get; }
        ISqlFragmentBuilder SqlBuilder { get; }
        IEnumerable<SqlStatementPartSelect> SelectColumns { get; }
        IEnumerable<(string rowIdColumnName, string resultClassProperty)> RowIdsForMappedProperties { get; }
    }
    
    public struct SqlStatementPartJoin
    {
        public string Alias { get; }
        public IEnumerable<string> QueryObjectReferences { get; }

        public SqlStatementPartJoin(string alias, IEnumerable<string> queryObjectReferences)
        {
            Alias = alias;
            QueryObjectReferences = queryObjectReferences;
        }
    }
    
    public struct SqlStatementPartSelect
    {
        public readonly bool IsRowId;
        public readonly Type CellDataType;
        public readonly string Alias;
        public readonly (string table, string column, string aggregatedTo)[] RepresentsColumns;
        public readonly ConstructorInfo[] ArgConstructors;

        public SqlStatementPartSelect(bool isRowId, Type cellDataType, string alias, (string table, string column, string aggregatedTo)[] representsColumns, ConstructorInfo[] argConstructors)
        {
            IsRowId = isRowId;
            CellDataType = cellDataType;
            Alias = alias;
            RepresentsColumns = representsColumns;
            ArgConstructors = argConstructors;
        }
    }
}