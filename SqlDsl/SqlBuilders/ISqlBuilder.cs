using SqlDsl.Query;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    public interface ISqlBuilder
    {
        IEnumerable<(string alias, string sql)> Joins { get; }
        
        string PrimaryTableAlias { get; }
        
        string InnerQueryAlias { get; }

        void SetPrimaryTable(string tableName, string alias);
        
        void SetPrimaryTable(ISqlBuilder table, string alias);

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
