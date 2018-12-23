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

        public QueryParts(string beforeWhereSql, string whereSql, string afterWhereSql)
        {
            BeforeWhereSql = beforeWhereSql;
            WhereSql = whereSql;
            AfterWhereSql = afterWhereSql;
        }

        public string Assemble(Func<string, string> map = null)
        {
            if (map == null) map = Identity;

            return map(BeforeWhereSql) +
                map(WhereSql) +
                map(AfterWhereSql);
        }
    }
}