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
            (IEnumerable<(string name, MemberInfo token, ParameterExpression reference)> tables, Expression equality, IList<object> paramaters) equalityStatement, 
            string joinTableAlias = null);

        void SetWhere(IEnumerable<(string name, MemberInfo token, ParameterExpression reference)> tables, Expression equality, IList<object> paramaters);

        (string querySetupSql, string querySql) ToSqlString();
    }
}
