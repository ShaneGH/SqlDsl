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
        SqlString ToSqlString(IEnumerable<string> selectColumnAliases = null);
    }

    public class SqlString
    {
        public readonly string QuerySetupSql;
        public readonly string BeforeWhereSql;
        public readonly string WhereSql;
        public readonly string AfterWhereSql;
        public readonly string QueryTeardownSql;
        public readonly bool TeardownSqlCanBeInlined;

        public SqlString(string querySetupSql, string beforeWhereSql, string whereSql, string afterWhereSql, string queryTeardownSql, bool teardownSqlCanBeInlined)
        {
            QuerySetupSql = querySetupSql;
            BeforeWhereSql = beforeWhereSql;
            WhereSql = whereSql;
            AfterWhereSql = afterWhereSql;
            QueryTeardownSql = queryTeardownSql;
            TeardownSqlCanBeInlined = teardownSqlCanBeInlined;
        }
    }
}
