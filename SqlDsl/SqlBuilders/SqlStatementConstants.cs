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
        /// <summary>
        /// The column name for table row ids
        /// </summary>
        public const string RowIdName = "##rowid";
        
        /// <summary>
        /// A column prefix to use internally if the resulting columns should actually not have a prefix
        /// (prevents naming clashes)
        /// </summary>
        public const string RootObjectAlias = "##root";
    }
}
