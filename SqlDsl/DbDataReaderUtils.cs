using System;
using System.Collections.Generic;
using System.Data;
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
        public static async Task<IEnumerable<IDataRow>> GetRowsAsync(this Func<IDataReader, IDataRow> constructor, DbDataReader reader)
        {
            var rows = new List<IDataRow>(16);

            (bool, IDataRow) row;
            while ((row = await constructor.GetRowAsync(reader).ConfigureAwait(false)).Item1)
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
        public static IEnumerable<IDataRow> GetRows(this Func<IDataReader, IDataRow> constructor, DbDataReader reader)
        {
            (bool, IDataRow) row;
            while ((row = constructor.GetRow(reader)).Item1)
                yield return row.Item2;
        }

        /// <summary>
        /// Read the next row in the query
        /// </summary>
        /// <returns> hasRow: if true, row will be populated, there may be more rows to read; 
        /// if false, row is null, there are no more rows to read
        /// row: the data from a row, ordered as the database returned it
        /// </returns>
        public static async Task<(bool hasRow, IDataRow row)> GetRowAsync(this Func<IDataReader, IDataRow> constructor, DbDataReader reader)
        {
            // read the next row
            if (!(await reader.ReadAsync().ConfigureAwait(false)))
                return (false, null);

            return (
                true,
                constructor(reader));
        }

        /// <summary>
        /// Read the next row in the query
        /// </summary>
        /// <returns> hasRow: if true, row will be populated, there may be more rows to read; 
        /// if false, row is null, there are no more rows to read
        /// row: the data from a row, ordered as the database returned it
        /// </returns>
        public static (bool hasRow, IDataRow row) GetRow(this Func<IDataReader, IDataRow> constructor, DbDataReader reader)
        {
            // read the next row
            if (!reader.Read())
                return (false, null);
                
            return (
                true,
                constructor(reader));
        }

        /// <summary>
        /// Get the types of the fields of this data reader
        /// </summary>
        public static Type[] GetFieldTypes(this DbDataReader reader)
        {
            var output = new Type[reader.VisibleFieldCount];
            for (var i = 0; i < output.Length; i++)
            {
                output[i] = reader.GetFieldType(i);
            }

            return output;
        }
    }
}
