using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{    
    /// <summary>
    /// Constants for use in sql queries
    /// </summary>
    public static class SqlStatementConstants
    {
        public const string RowIdName = "##rowid";
        public const string RootObjectAlias = "##root";
    }
}
