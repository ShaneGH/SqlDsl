using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using SqlDsl.Sqlite;
using SqlDsl.Utils;

namespace SqlDsl.UnitTests.FullPathTests.Environment
{
    public class InitSqliteDatabase : InitDatabaseBase
    {
        public InitSqliteDatabase(
            IEnumerable<Person> people = null,
            IEnumerable<PersonsData> peoplesData = null,
            IEnumerable<PersonClass> personClasses = null,
            IEnumerable<Class> classes = null,
            IEnumerable<ClassTag> classTags = null,
            IEnumerable<Tag> tags = null,
            IEnumerable<Purchase> purchases = null,
            IEnumerable<TableWithOneRowAndOneColumn> tablesWithOneColumn = null,
            IEnumerable<DataDump> dataDump = null,
            IEnumerable<TestDataTable> testData = null)
            : base(
                BuildDependenciesTypes(),
                people,
                peoplesData,
                personClasses,
                classes,
                classTags,
                tags,
                purchases,
                tablesWithOneColumn,
                dataDump,
                testData)
        {
        }

        static Dependencies BuildDependenciesTypes()
        {
            return new Dependencies(
                new SqliteSyntax(),
                new DataTypes
                {
                    String =  "TEXT",
                    DateTime = "INTEGER",
                    DateTime_Null = "INTEGER",
                    Bool = "INTEGER",
                    Bool_Null = "INTEGER",
                    Int = "INTEGER",
                    Int_Null = "INTEGER",
                    Long = "INTEGER",
                    Long_Null = "INTEGER",
                    Float = "REAL",
                    Float_Null = "REAL",
                    ByteArray = "BLOB",
                    Byte = "INTEGER",
                    Byte_Null = "INTEGER",
                    SByte = "INTEGER",
                    SByte_Null = "INTEGER",
                    Short = "INTEGER",
                    Short_Null = "INTEGER",
                    UShort = "INTEGER",
                    UShort_Null = "INTEGER",
                    UInt = "INTEGER",
                    UInt_Null = "INTEGER",
                    ULong = "INTEGER",
                    ULong_Null = "INTEGER",
                    Double = "REAL",
                    Double_Null = "REAL",
                    Decimal = "REAL",
                    Decimal_Null = "REAL",
                    Char = "TEXT",
                    Char_Null = "TEXT",
                    Guid = "BLOB",
                    Guid_Null = "BLOG",
                    Enum = "INTEGER",
                    Enum_Null = "INTEGER"
                },
                _ => 
                {
                    var location = GetDbLocation();
                    if (File.Exists(location)) File.Delete(location);
                },
                _ => new Connection());
        }

        public static string GetDbLocation()
        {
            var locationParts = new Regex(@"\\|/").Split(typeof(InitData).Assembly.Location).ToArray();
            return locationParts.Take(locationParts.Length - 1).JoinString(@"\") + @"\data.db";
        }

        public static SqliteConnection CreateSqliteConnection() => new SqliteConnection($@"Data Source={GetDbLocation()}");

        class Connection : IConnection
        {
            readonly SqliteConnection _connection = CreateSqliteConnection();

            public void Dispose()
            {
                _connection.Dispose();
            }

            public void ExecuteCommand(string sql, IEnumerable<object> paramaters)
            {
                var cmd = _connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.Parameters.AddRange(paramaters.Select((p, i) => 
                {
                    var param = cmd.CreateParameter();
                    param.ParameterName = "p" + i;
                    param.Value = p == null ? DBNull.Value : p;
                    return param;
                }));
                
                cmd.ExecuteNonQuery();
            }

            public void Open()
            {
                _connection.Open();
            }
        }
    }
}
