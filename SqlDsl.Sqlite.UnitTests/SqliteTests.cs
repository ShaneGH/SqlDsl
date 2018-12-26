using System;
using SqlDsl.UnitTests.SqlFlavours;
using SqlDsl.Utils;
using NUnit.Framework;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Linq;
using Microsoft.Data.Sqlite;
using System.IO;
using SqlDsl.Dsl;
using SqlDsl.Query;
using SqlDsl.SqlBuilders;

namespace SqlDsl.Sqlite.UnitTests
{
    [TestFixture]
    public class SqliteTests : SqlFragmentBuilderTestBase
    {
        protected override Type GetTypeOfExceptionForEmptyIn() => null;

        readonly string DbFileName = GetDbFileName();

        static string GetDbFileName()
        {
            return Path.GetTempFileName();
        }
        
        public SqliteConnection CreateConnection()
        {
            var conn = new SqliteConnection($@"Data Source={DbFileName}");
            conn.Open();

            return conn;
        }

        public override void DisposeOfExecutor(IExecutor executor)
        {
            if (!(executor is SqliteExecutor))
                throw new InvalidOperationException("Expecting executor to be SqliteExecutor.");

            var ex = executor as SqliteExecutor;
            ex.Connection.Dispose();
        }

        public override void CreateDb(TableDescriptor table)
        {
            DropDb();
            
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
            if (File.Exists(DbFileName)) File.Delete(DbFileName);
        }

        public override IExecutor CreateExecutor() => new SqliteExecutor(CreateConnection());

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

        public override ISqlSyntax GetSyntax() => new SqliteSyntax();
    }
}
