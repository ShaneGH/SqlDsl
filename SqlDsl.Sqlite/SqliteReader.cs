
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
        /// row: the data from a row, ordered as the database returned it
        /// </returns>
        public async Task<(bool hasRow, object[] row)> GetRowAsync()
        {
            // read the next row
            if (!(await DataReader.ReadAsync().ConfigureAwait(false)))
                return (false, null);

            // load row into array
            var vals = new object[DataReader.FieldCount];
            DataReader.GetValues(vals);
                
            return (true, vals);
        }

        /// <summary>
        /// Read the next row in the query
        /// </summary>
        /// <returns> hasRow: if true, row will be populated, there may be more rows to read; 
        /// if false, row is null, there are no more rows to read
        /// row: the data from a row, ordered as the database returned it
        /// </returns>
        public (bool hasRow, object[] row) GetRow()
        {
            // read the next row
            if (!DataReader.Read())
                return (false, null);

            // load row into array
            var vals = new object[DataReader.FieldCount];
            DataReader.GetValues(vals);
                
            return (true, vals);
        }
    }
}
