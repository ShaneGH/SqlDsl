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

        readonly string Sql;
        readonly string TeardownSql;

        public QueryParts(string sql, string teardownSql)
        {
            Sql = sql;
            TeardownSql = teardownSql;
        }

        public (string sql, string teardownSql) Assemble(Func<string, string> map = null)
        {
            if (map == null) map = Identity;

            return (map(Sql), TeardownSql);
        }
    }
}