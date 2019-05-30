using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;
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
            IEnumerable<DataDump> dataDump = null)
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
                dataDump)
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
                    Enum = "INTEGER",
                    
                    GetString =  (x, y) => x == null ? "NULL" : "'" + x + "'",
                    GetDateTime = (x, y) => x.Ticks.ToString(),
                    GetDateTime_Null = (x, y) => x == null ? "NULL" : x.Value.Ticks.ToString(),
                    GetBool = (x, y) => x ? "1" : "0",
                    GetBool_Null = (x, y) => x == null ? "NULL" : x.Value ? "1" : "0",
                    GetInt = (x, y) => x.ToString(),
                    GetInt_Null = (x, y) => x == null ? "NULL" : x.ToString(),
                    GetLong = (x, y) => x.ToString(),
                    GetLong_Null = (x, y) => x == null ? "NULL" : x.ToString(),
                    GetFloat = (x, y) => x.ToString(),
                    GetFloat_Null = (x, y) => x == null ? "NULL" : x.ToString(),
                    GetByteArray = (x, y) => 
                    {
                        y.Add(x);
                        return "@p" + (y.Count - 1);   
                    },
                    GetEnum = (x, y) => x.ToString()
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
