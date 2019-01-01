using System;
using System.Collections.Generic;
using SqlDsl.SqlBuilders;
using System.Threading.Tasks;
using System.Linq;
using SqlDsl.Utils;
using SqlDsl.DataParser;
using SqlDsl.SqlBuilders.SqlStatementParts;
using System.Reflection;

namespace SqlDsl.Query
{
    public class QueryParts
    {
        static readonly Func<string, string> Identity = x => x;

        readonly string BeforeWhereSql;
        readonly string WhereSql;
        readonly string AfterWhereSql;
        readonly string TeardownSql;

        public QueryParts(string beforeWhereSql, string whereSql, string afterWhereSql, string teardownSql)
        {
            BeforeWhereSql = beforeWhereSql;
            WhereSql = whereSql;
            AfterWhereSql = afterWhereSql;
            TeardownSql = teardownSql;
        }

        public (string sql, string teardownSql) Assemble(Func<string, string> map = null)
        {
            if (map == null) map = Identity;

            return (map(BeforeWhereSql) +
                map(WhereSql) +
                map(AfterWhereSql), TeardownSql);
        }
    }
}