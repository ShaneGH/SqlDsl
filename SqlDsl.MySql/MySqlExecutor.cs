
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace SqlDsl.MySql
{
    /// <summary>
    /// An object which can execute a MySql query
    /// </summary>
    public class MySqlExecutor : IExecutor
    {
        /// <summary>
        /// The connection
        /// </summary>
        public MySqlConnection Connection;

        private static readonly Regex AllowUserVariables1 = new Regex(@"AllowUserVariables\s*=\s*True", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex AllowUserVariables2 = new Regex(@"Allow\s+User\s+Variables\s*=\s*True", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        public MySqlExecutor(MySqlConnection connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));

            if (!AllowUserVariables1.IsMatch(Connection.ConnectionString) && !AllowUserVariables2.IsMatch(Connection.ConnectionString))
                throw new InvalidOperationException("Your connection string must include \"Allow User Variables=True\".");
        }

        /// <inheritdoc />
        public async Task<IReader> ExecuteAsync(string sql, IEnumerable<(string name, object value)> paramaters) =>
            new MySqlReader(await CreateCommand(sql, paramaters).ExecuteReaderAsync().ConfigureAwait(false));

        /// <inheritdoc />
        public IReader Execute(string sql, IEnumerable<(string name, object value)> paramaters) =>
            new MySqlReader(CreateCommand(sql, paramaters).ExecuteReader());

        /// <inheritdoc />
        public Task ExecuteCommandAsync(string sql, IEnumerable<(string name, object value)> paramaters) =>
            CreateCommand(sql, paramaters).ExecuteNonQueryAsync();

        /// <inheritdoc />
        public void ExecuteCommand(string sql, IEnumerable<(string name, object value)> paramaters) =>
            CreateCommand(sql, paramaters).ExecuteNonQuery();

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
