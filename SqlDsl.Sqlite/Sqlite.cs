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
        public static ITable<TResult> Sqlite<TResult>(this QueryBuilder builder)
        {
            return new QueryBuilder<TResult>(SqliteSyntax);
        }

        /// <summary>
        /// Build a Sqlite query with args
        /// </summary>
        public static ITable<TArgs, TResult> Sqlite<TArgs, TResult>(this QueryBuilder builder)
        {
            return new QueryBuilder<TArgs, TResult>(SqliteSyntax);
        }
    }
}
