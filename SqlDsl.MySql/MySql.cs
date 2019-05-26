using SqlDsl.Dsl;
using SqlDsl.Query;
using SqlDsl.MySql;

namespace SqlDsl
{
    /// <summary>
    /// Utils to build and execute MySql queries
    /// </summary>
    public static class MySqlUtils
    {
        static readonly MySqlSyntax MySqlSyntax = new MySqlSyntax();

        /// <summary>
        /// Build a MySql query
        /// </summary>
        /// <param name="strictJoins">If set to true, every join added to the SqlDsl query will also be added to the Sql query.
        /// If false, joins which are not used in a mapping, WHERE clause, ON clause etc... will be automatically removed</param>
        public static ISqlSelect<TResult> MySql<TResult>(this QueryBuilder builder, bool strictJoins = true)
        {
            return new SqlSelect<TResult>(MySqlSyntax, strictJoins);
        }

        /// <summary>
        /// Build a MySql query with args
        /// </summary>
        /// <param name="strictJoins">If set to true, every join added to the SqlDsl query will also be added to the Sql query.
        /// If false, joins which are not used in a mapping, WHERE clause, ON clause etc... will be automatically removed</param>
        public static ISqlSelect<TArgs, TResult> MySql<TArgs, TResult>(this QueryBuilder builder, bool strictJoins = true)
        {
            return new SqlSelect<TArgs, TResult>(MySqlSyntax, strictJoins);
        }
    }
}
