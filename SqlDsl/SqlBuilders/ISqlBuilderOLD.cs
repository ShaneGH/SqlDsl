using SqlDsl.Query;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    public interface ISqlBuilderOLD
    {
        IEnumerable<(string alias, string sql)> Joins { get; }
        
        string PrimaryTableAlias { get; }
        
        string InnerQueryAlias { get; }

        IEnumerable<(string columnName, string rowIdColumnName)> RowIdMap { get; }

        IEnumerable<(string columnName, string tableName, string alias)> RowIdSelectColumns { get; }

        IEnumerable<string> SelectColumns { get; }

        void SetPrimaryTable(string tableName, string alias);
        
        void SetPrimaryTable(ISqlBuilderOLD table, string alias);

        void AddSelectColumn(string columnName, string tableName = null, string alias = null);

        void AddJoin(
            JoinType joinType, 
            string joinTable, 
            ParameterExpression queryRootParam, 
            ParameterExpression joinTableParam,
            Expression equalityStatement, 
            IList<object> paramaters, 
            string joinTableAlias);

        void SetWhere(ParameterExpression queryRoot, Expression equality, IList<object> paramaters);

        (string querySetupSql, string querySql) ToSqlString();
    }
}
