using System;
using System.Collections.Generic;
using System.Linq;
using SqlDsl.Query;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.TSql
{
    /// <summary>
    /// Class for building TSql statements
    /// </summary>
    public class TSqlSyntax : SqlSyntaxBase
    {
        /// <summary>
        /// Wrap a table name in parenthesis which protects against illegal characters: []
        /// </summary>
        public override string WrapTable(string table) => $"[{table}]";

        /// <summary>
        /// Wrap a column name in parenthesis which protects against illegal characters: []
        /// </summary>
        public override string WrapColumn(string column) => $"[{column}]";

        /// <summary>
        /// Wrap a table or column alias in parenthesis which protects against illegal characters: []
        /// </summary>
        public override string WrapAlias(string alias) => $"[{alias}]";
    }
}
