﻿
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
        public async Task<IReader> ExecuteAsync(string sql, IEnumerable<(string name, object value)> paramaters) =>
            new SqliteReader(await CreateCommand(sql, paramaters).ExecuteReaderAsync().ConfigureAwait(false));

        /// <summary>
        /// Execute a sqlite query and return a reader to read results
        /// </summary>
        public IReader Execute(string sql, IEnumerable<(string name, object value)> paramaters) =>
            new SqliteReader(CreateCommand(sql, paramaters).ExecuteReader());

        /// <summary>
        /// Create a sql command with the given sql and parameters
        /// </summary>
        SqliteCommand CreateCommand(string sql, IEnumerable<(string name, object value)> paramaters)
        {
            var command = Connection.CreateCommand();
            command.CommandText = sql;

            foreach (var p in paramaters)
            {
                command.Parameters.Add(new SqliteParameter(p.name, p.value ?? DBNull.Value));
            }

            return command;
        }
    }
}
