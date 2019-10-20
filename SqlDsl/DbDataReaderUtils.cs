using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using SqlDsl.DataParser.DataRow;

namespace SqlDsl
{
    public static class DbDataReaderUtils
    {
        /// <summary>
        /// Read all rows from an DbDataReader
        /// </summary>
        public static async Task<IEnumerable<object[]>> GetRowsAsync(this DbDataReader reader)
        {
            var rows = new List<object[]>(16);

            (bool, object[]) row;
            while ((row = await reader.GetRowAsync().ConfigureAwait(false)).Item1)
            {
                var types = new Type[reader.FieldCount];
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    types[i] = reader.GetFieldType(i);
                }

                rows.Add(row.Item2);
            }

            return rows;
        }

        /// <summary>
        /// Read all rows from an IReader
        /// </summary>
        public static IEnumerable<object[]> GetRows(this DbDataReader reader)
        {
            var rows = new List<object[]>();

            (bool, object[]) row;
            while ((row = reader.GetRow()).Item1)
                yield return row.Item2;
        }

        /// <summary>
        /// Read the next row in the query
        /// </summary>
        /// <returns> hasRow: if true, row will be populated, there may be more rows to read; 
        /// if false, row is null, there are no more rows to read
        /// row: the data from a row, ordered as the database returned it
        /// </returns>
        public static async Task<(bool hasRow, object[] row)> GetRowAsync(this DbDataReader reader)
        {
            // read the next row
            if (!(await reader.ReadAsync().ConfigureAwait(false)))
                return (false, null);

            // load row into array
            var vals = new object[reader.FieldCount];
            reader.GetValues(vals);
                
            return (true, vals);
        }

        /// <summary>
        /// Read the next row in the query
        /// </summary>
        /// <returns> hasRow: if true, row will be populated, there may be more rows to read; 
        /// if false, row is null, there are no more rows to read
        /// row: the data from a row, ordered as the database returned it
        /// </returns>
        public static (bool hasRow, object[] row) GetRow(this DbDataReader reader)
        {
            // read the next row
            if (!reader.Read())
                return (false, null);

            // load row into array
            var vals = new object[reader.FieldCount];
            reader.GetValues(vals);
                
            return (true, vals);
        }
    }
}
