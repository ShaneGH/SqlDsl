using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SqlDsl
{
    /// <summary>
    /// An object which can read results from an executed query
    /// </summary>
    public interface IReader : IDisposable
    {
        /// <summary>
        /// Read the next row in the query
        /// </summary>
        /// <returns> hasRow: if true, row will be populated, there may be more rows to read; 
        /// if false, row is null, there are no more rows to read
        /// row: the data from a row, ordered as the database returned it
        /// </returns>
        Task<(bool hasRow, object[] row)> GetRowAsync();

        /// <summary>
        /// Read the next row in the query
        /// </summary>
        /// <returns> hasRow: if true, row will be populated, there may be more rows to read; 
        /// if false, row is null, there are no more rows to read
        /// row: the data from a row, ordered as the database returned it
        /// </returns>
        (bool hasRow, object[] row) GetRow();
    }

    public static class IReaderUtils
    {
        /// <summary>
        /// Read all rows from an IReader
        /// </summary>
        public static async Task<IEnumerable<object[]>> GetRowsAsync(this IReader reader)
        {
            var rows = new List<object[]>();

            (bool, object[]) row;
            while ((row = await reader.GetRowAsync().ConfigureAwait(false)).Item1)
                rows.Add(row.Item2);

            return rows;
        }

        /// <summary>
        /// Read all rows from an IReader
        /// </summary>
        public static IEnumerable<object[]> GetRows(this IReader reader)
        {
            var rows = new List<object[]>();

            (bool, object[]) row;
            while ((row = reader.GetRow()).Item1)
                rows.Add(row.Item2);

            return rows;
        }
    }
}
