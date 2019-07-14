using SqlDsl.Dsl;
using SqlDsl.Query;
using SqlDsl.TSql;

namespace SqlDsl
{
    /// <summary>
    /// Utils to build and execute TSql queries
    /// </summary>
    public static class TSqlUtils
    {
        static readonly TSqlSyntax TSqlSyntax = new TSqlSyntax();

        /// <summary>
        /// Build a TSql query
        /// </summary>
        /// <param name="strictJoins">If set to true, every join added to the SqlDsl query will also be added to the Sql query.
        /// If false, joins which are not used in a mapping, WHERE clause, ON clause etc... will be automatically removed</param>
        /// <param name="settings">The settings for the query. If null, TSqlSettings.Default are used</param>
        public static ISqlSelect<TResult> TSql<TResult>(this QueryBuilder builder, bool strictJoins = true)
        {
            return new SqlSelect<TResult>(TSqlSyntax, strictJoins);
        }

        /// <summary>
        /// Build a TSql query with args
        /// </summary>
        /// <param name="strictJoins">If set to true, every join added to the SqlDsl query will also be added to the Sql query.
        /// If false, joins which are not used in a mapping, WHERE clause, ON clause etc... will be automatically removed</param>
        /// <param name="settings">The settings for the query. If null, TSqlSettings.Default are used</param>
        public static ISqlSelect<TArgs, TResult> TSql<TArgs, TResult>(this QueryBuilder builder, bool strictJoins = true)
        {
            return new SqlSelect<TArgs, TResult>(TSqlSyntax, strictJoins);
        }
    }
}
