
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SqlDsl.Sqlite
{
    /// <summary>
    /// An object which can execute a sqlite query
    /// </summary>
    public class SqliteExecutor : IExecutor
    {
        /// <summary>
        /// The connection
        /// </summary>
        public SqliteConnection Connection;

        public SqliteExecutor(SqliteConnection connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        /// <summary>
        /// Execute a sqlite query and return a reader to read results
        /// </summary>
        public async Task<IReader> ExecuteAsync(string sql, IEnumerable<object> paramaters)
        {
            var command = Connection.CreateCommand();
            command.CommandText = sql;

            var i = 0;
            foreach (var p in paramaters)
            {
                command.Parameters.Add(new SqliteParameter("p" + (i++), p));
            }

            return new SqliteReader(await command.ExecuteReaderAsync());
        }
    }
}
