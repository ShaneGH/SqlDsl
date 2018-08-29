using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using SqlDsl.Utils;

namespace SqlDsl
{
    /// <summary>
    /// An object which can read results from an executed query
    /// </summary>
    public interface IReader
    {
        /// <summary>
        /// Read the next row in the query
        /// </summary>
        /// <returns> hasRow: if true, row will be populated, there may be more rows to read; 
        /// if false, row is null, there are no more rows to read
        /// row: a group of key value pairs where the key is the column name and the value is the cell value
        /// </returns>
        Task<(bool hasRow, List<(string key, object value)> row)> GetRowAsync();
    }

    public static class IReaderUtils
    {
        /// <summary>
        /// Read all rows from an IReader
        /// </summary>
        public static async Task<IEnumerable<List<(string key, object value)>>> GetRowsAsync(this IReader reader)
        {
            var rows = new List<List<(string, object)>>();

            (bool, IEnumerable<(string, object)>) row;
            while ((row = await reader.GetRowAsync()).Item1)
            {
                rows.Add(row.Item2.ToList());
            }

            return rows;
        }
    }
}
