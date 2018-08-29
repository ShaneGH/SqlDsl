﻿
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SqlDsl.Sqlite
{
    /// <summary>
    /// An object which can read results from an executed sqlite query
    /// </summary>
    public class SqliteReader : IReader
    {
        /// <summary>
        /// The reader to retrieve data from
        /// </summary>
        readonly SqliteDataReader DataReader;

        public SqliteReader(SqliteDataReader reader)
        {
            DataReader = reader ?? throw new ArgumentNullException(nameof(reader));
        }

        /// <summary>
        /// Read the next row in the query
        /// </summary>
        /// <returns> hasRow: if true, row will be populated, there may be more rows to read; 
        /// if false, row is null, there are no more rows to read
        /// row: a group of key value pairs where the key is the column name and the value is the cell value
        /// </returns>
        public async Task<(bool hasRow, List<(string key, object value)> row)> GetRowAsync()
        {
            // read the next row
            if (!(await DataReader.ReadAsync()))
                return (false, null);

            // load row into array
            var vals = new object[DataReader.FieldCount];
            DataReader.GetValues(vals);
            
            // add row values to list along with column names
            var output = new List<(string, object)>();
            for (var i = 0; i < vals.Length; i++)
                output.Add((DataReader.GetName(i), vals[i]));
                
            return (true, output);
        }
    }
}
