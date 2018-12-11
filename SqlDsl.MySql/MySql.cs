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
        /// <summary>
        /// Build a MySql query
        /// </summary>
        public static ITable<TResult> MySql<TResult>(this QueryBuilder builder)
        {
            return new QueryBuilder<MySqlBuilder, TResult>();
        }

        /// <summary>
        /// Build a MySql query with args
        /// </summary>
        public static ITable<TArgs, TResult> MySql<TArgs, TResult>(this QueryBuilder builder)
        {
            return new QueryBuilder<MySqlBuilder, TArgs, TResult>();
        }
    }
}
