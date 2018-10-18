using System;
using SqlDsl.UnitTests.SqlFlavours;
using SqlDsl.Utils;
using NUnit.Framework;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Linq;
using Microsoft.Data.Sqlite;
using System.IO;

namespace SqlDsl.Sqlite.UnitTests
{
    [TestFixture]
    public class SqliteTests : SqlFragmentBuilderTestBase<SqliteBuilder>
    {
        readonly string DbFileName = $"SqliteTests, {DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss")}.db";
        readonly Lazy<SqliteConnection> ExecutorConnection;

        public SqliteTests()
        {
            ExecutorConnection = new Lazy<SqliteConnection>(CreateConnection);
        }

        public string GetDbLocation()
        {
            var locationParts = new Regex(@"\\|/").Split(typeof(SqliteTests).Assembly.Location).ToArray();
            return locationParts.Take(locationParts.Length - 1).JoinString(@"\") + @"\" + DbFileName;
        }
        
        public SqliteConnection CreateConnection()
        {
            var conn = new SqliteConnection($@"Data Source={GetDbLocation()}");
            conn.Open();

            return conn;
        }

        public override void CreateDb(TableDescriptor table)
        {
            var location = GetDbLocation();
            if (File.Exists(location)) File.Delete(location);
            
            var columns = table.Columns
                .Select(col => $"[{col.Name}] {GetColType(col.DataType)} {GetNullable(col.Nullable)}");

            var sql = new[]
            {
                $"CREATE TABLE {table.Name} (",
                columns.JoinString(",\n"),
                ")"
            }.JoinString("\n");

            using (var conn = CreateConnection())
            {   
                conn.Open();

                var cmd = conn.CreateCommand();
                Console.WriteLine($" * Adding schema");
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
                Console.WriteLine($" * Schema added");
            }
        }

        string GetColType(Type type)
        {
            if (type == typeof(byte) ||
                type == typeof(sbyte) ||
                type == typeof(bool) ||
                type == typeof(short) ||
                type == typeof(int) ||
                type == typeof(long) ||
                type == typeof(ushort) ||
                type == typeof(uint) ||
                type == typeof(ulong) ||
                type == typeof(DateTime))
                return "INTEGER";
                
            if (type == typeof(float) ||
                type == typeof(double) ||
                type == typeof(decimal))
                return "REAL";
                
            if (type == typeof(char) ||
                type == typeof(char[]) ||
                type == typeof(List<char>) ||
                type == typeof(IEnumerable<char>) ||
                type == typeof(string))
                return "TEXT";
                
            if (type == typeof(Guid) ||
                type == typeof(byte[]) ||
                type == typeof(List<byte>) ||
                type == typeof(IEnumerable<byte>))
                return "BLOB";

            throw new InvalidOperationException($"Unsupported data type: {type}");
        }

        string GetNullable(bool nullable)
        {
            return nullable ? "NULL" : "NOT NULL";
        }

        public override void DropDb()
        {
            if (ExecutorConnection.IsValueCreated) ExecutorConnection.Value.Dispose();

            var location = GetDbLocation();
            if (File.Exists(location)) File.Delete(location);
        }

        public override IExecutor GetExecutor() => new SqliteExecutor(ExecutorConnection.Value);

        public override void SeedDb(string tableName, IEnumerable<IEnumerable<KeyValuePair<string, object>>> rows)
        {
            using (var conn = CreateConnection())
            {   
                conn.Open();
                foreach (var row in rows)
                {
                    var query = new []
                    {
                        $"INSERT INTO {tableName}",
                        "(",
                        row.Select(r => r.Key).JoinString(","),
                        ")",
                        "VALUES",
                        "(",
                        row.Select((_, i) => "@p" + i).JoinString(","),
                        ")"
                    }.JoinString("\n");

                    var cmd = conn.CreateCommand();
                    Console.WriteLine($" * Adding data row");
                    cmd.CommandText = query;
                    cmd.Parameters.AddRange(row.Select((v, i) => new SqliteParameter("p" + (i++), ConvertData(v.Value) ?? DBNull.Value)));

                    cmd.ExecuteNonQuery();
                    Console.WriteLine($" * Data row added");
                }
            }
        }

        static object ConvertData(object data)
        {
            if (data is List<byte>) return (data as List<byte>).ToArray();
            if (data is char) return new String(new[]{(char)data});
            if (data is List<char>) return new String((data as List<char>).ToArray());
            if (data is char[]) return new String(data as char[]);

            return data;
        }
    }
}
