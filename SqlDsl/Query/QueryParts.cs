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
        public readonly string BeforeWhereSql;
        public readonly string WhereSql;
        public readonly string AfterWhereSql;

        public QueryParts(string beforeWhereSql, string whereSql, string afterWhereSql)
        {
            BeforeWhereSql = beforeWhereSql;
            WhereSql = whereSql;
            AfterWhereSql = afterWhereSql;
        }
    }
}