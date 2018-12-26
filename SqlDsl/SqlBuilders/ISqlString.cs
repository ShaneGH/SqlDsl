using SqlDsl.Query;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    /// <summary>
    /// Build sql and expose it's metadata
    /// </summary>
    public interface ISqlString
    {
        /// <summary>
        /// Generate sql. Setup sql will be executd before querySql, but in the same sql script
        /// </summary>
        (string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql) ToSqlString();
    }
}
