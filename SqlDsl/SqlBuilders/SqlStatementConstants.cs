using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

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
        
        /// <summary>
        /// A string to prefix parameters which will be split into multiple values for an IN statement
        /// </summary>
        public const string ParamArrayFlag = "##array";
        
        /// <summary>
        /// The alias of a column in a select statement which only returns one column
        /// </summary>
        public const string SingleColumnAlias = "#c";

        
            public const string ConstructorArgPrefixAlias = "##ca";
        
        private static class ConstructorArgs
        {
            /// <summary>
            /// A string to prefix columns names, whose data will be used as a constructor arg
            /// </summary>
            const string ConstructorArgPrefixAlias = "##ca";

            static readonly Regex LooseConstructorArgRegex = new Regex($"{Regex.Escape(ConstructorArgPrefixAlias)}\\d+", RegexOptions.Compiled);
            public static int CountConstructorArgs(string value) => LooseConstructorArgRegex.Matches(value).Count;

            static readonly Regex TightConstructorArgRegex = new Regex($"^{Regex.Escape(ConstructorArgPrefixAlias)}(\\d+)$", RegexOptions.Compiled);
            public static bool IsConstructorArg(string value) => TightConstructorArgRegex.IsMatch(value);

            public static string BuildConstructorArg(int index) => $"{ConstructorArgPrefixAlias}{index}";

            public static bool TryGetConstructorArgIndex(string value, out int index)
            {
                var match = TightConstructorArgRegex.Match(value);
                if (!match.Success)
                {
                    index = 0;
                    return false;
                }

                index = int.Parse(match.Groups[1].Captures[0].Value);
                return true;
            }
        }
    }
}
