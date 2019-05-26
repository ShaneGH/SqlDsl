using SqlDsl.Dsl;
using SqlDsl.Query;
using SqlDsl.Sqlite;

namespace SqlDsl
{
    /// <summary>
    /// Utils to build and execute Sqlite queries
    /// </summary>
    public static class SqliteUtils
    {
        static readonly SqliteSyntax SqliteSyntax = new SqliteSyntax();

        /// <summary>
        /// Build a Sqlite query
        /// </summary>
        /// <param name="strictJoins">If set to true, every join added to the SqlDsl query will also be added to the Sql query.
        /// If false, joins which are not used in a mapping, WHERE clause, ON clause etc... will be automatically removed</param>
        public static ISqlSelect<TResult> Sqlite<TResult>(this QueryBuilder builder, bool strictJoins = true)
        {
            return new SqlSelect<TResult>(SqliteSyntax, strictJoins);
        }

        /// <summary>
        /// Build a Sqlite query with args
        /// </summary>
        /// <param name="strictJoins">If set to true, every join added to the SqlDsl query will also be added to the Sql query.
        /// If false, joins which are not used in a mapping, WHERE clause, ON clause etc... will be automatically removed</param>
        public static ISqlSelect<TArgs, TResult> Sqlite<TArgs, TResult>(this QueryBuilder builder, bool strictJoins = true)
        {
            return new SqlSelect<TArgs, TResult>(SqliteSyntax, strictJoins);
        }
    }
}
