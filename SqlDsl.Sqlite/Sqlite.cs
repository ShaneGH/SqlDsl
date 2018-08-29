using SqlDsl.Query;
using SqlDsl.Sqlite;

namespace SqlDsl
{
    /// <summary>
    /// Utils to build and execute Sqlite queries
    /// </summary>
    public static class SqliteUtils
    {
        /// <summary>
        /// Build a Sqlite query
        /// </summary>
        public static ITable<TResult> Sqlite<TResult>(this QueryBuilder builder)
            where TResult: new()
        {
            return new QueryBuilder<SqliteBuilder, TResult>();
        }
    }
}
