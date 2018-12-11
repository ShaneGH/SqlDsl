using Microsoft.Data.MySql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SqlDsl.MySql
{
    /// <summary>
    /// An object which can execute a mysql query
    /// </summary>
    public class MySqlExecutor : IExecutor
    {
        /// <summary>
        /// The connection
        /// </summary>
        public MySqlConnection Connection;

        public MySqlExecutor(MySqlConnection connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        /// <summary>
        /// Execute a mysql query and return a reader to read results
        /// </summary>
        public async Task<IReader> ExecuteAsync(string sql, IEnumerable<(string name, object value)> paramaters) =>
            new MySqlReader(await CreateCommand(sql, paramaters).ExecuteReaderAsync().ConfigureAwait(false));

        /// <summary>
        /// Execute a mysql query and return a reader to read results
        /// </summary>
        public IReader Execute(string sql, IEnumerable<(string name, object value)> paramaters) =>
            new MySqlReader(CreateCommand(sql, paramaters).ExecuteReader());

        /// <summary>
        /// Create a sql command with the given sql and parameters
        /// </summary>
        MySqlCommand CreateCommand(string sql, IEnumerable<(string name, object value)> paramaters)
        {
            var command = Connection.CreateCommand();
            command.CommandText = sql;

            foreach (var p in paramaters)
            {
                command.Parameters.Add(new MySqlParameter(p.name, p.value ?? DBNull.Value));
            }

            return command;
        }
    }
}
