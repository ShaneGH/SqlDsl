using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data.Common;

namespace SqlDsl.TSql
{
    /// <summary>
    /// An object which can execute a TSql query
    /// </summary>
    public class TSqlExecutor : IExecutor
    {
        /// <summary>
        /// The connection
        /// </summary>
        public SqlConnection Connection;

        public TSqlExecutor(SqlConnection connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        /// <inheritdoc />
        public async Task<IReader> ExecuteAsync(string sql, IEnumerable<(string name, object value)> paramaters) =>
            new TSqlReader(await CreateCommand(sql, paramaters).ExecuteReaderAsync().ConfigureAwait(false));

        /// <inheritdoc />
        public IReader Execute(string sql, IEnumerable<(string name, object value)> paramaters) =>
            new TSqlReader(CreateCommand(sql, paramaters).ExecuteReader());

        /// <inheritdoc />
        public DbDataReader ExecuteDataReader(string sql, IEnumerable<(string name, object value)> paramaters) =>
            CreateCommand(sql, paramaters).ExecuteReader();

        /// <inheritdoc />
        public Task ExecuteCommandAsync(string sql, IEnumerable<(string name, object value)> paramaters) =>
            CreateCommand(sql, paramaters).ExecuteNonQueryAsync();

        /// <inheritdoc />
        public void ExecuteCommand(string sql, IEnumerable<(string name, object value)> paramaters) =>
            CreateCommand(sql, paramaters).ExecuteNonQuery();

        /// <summary>
        /// Create a sql command with the given sql and parameters
        /// </summary>
        SqlCommand CreateCommand(string sql, IEnumerable<(string name, object value)> paramaters)
        {
            var command = Connection.CreateCommand();
            command.CommandText = sql;

            foreach (var p in paramaters)
            {
                command.Parameters.Add(new SqlParameter(p.name, p.value ?? DBNull.Value));
            }

            return command;
        }
    }
}
