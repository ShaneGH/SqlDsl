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
        public static ISqlSelect<TResult> Sqlite<TResult>(this QueryBuilder builder)
        {
            return new SqlSelect<TResult>(SqliteSyntax);
        }

        /// <summary>
        /// Build a Sqlite query with args
        /// </summary>
        public static ISqlSelect<TArgs, TResult> Sqlite<TArgs, TResult>(this QueryBuilder builder)
        {
            return new SqlSelect<TArgs, TResult>(SqliteSyntax);
        }
    }
}
