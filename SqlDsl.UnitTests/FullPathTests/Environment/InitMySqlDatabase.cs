
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MySql.Data.MySqlClient;
using SqlDsl.MySql;

namespace SqlDsl.UnitTests.FullPathTests.Environment
{
    public class InitMySqlDatabase : InitDatabaseBase
    {
        public InitMySqlDatabase(
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
                new MySqlSyntax(),
                new DataTypes
                {
                    String =  "TEXT",
                    DateTime = "DATETIME",
                    DateTime_Null = "DATETIME",
                    Bool = "BOOL",
                    Bool_Null = "BOOL",
                    Int = "INT",
                    Int_Null = "INT",
                    Long = "BIGINT",
                    Long_Null = "BIGINT",
                    Float = "FLOAT",
                    Float_Null = "FLOAT",
                    ByteArray = "BLOB",
                    Enum = "SMALLINT",
                    
                    GetString =  (x, y) => x == null ? "NULL" : "'" + x + "'",
                    GetDateTime = (x, y) => 
                    {
                        y.Add(x);
                        return "@p" + (y.Count - 1);   
                    },
                    GetDateTime_Null = (x, y) => 
                    {
                        if (x == null) return "NULL";

                        y.Add(x);
                        return "@p" + (y.Count - 1);   
                    },
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
                () => 
                {
                    using (var conn = new MySqlConnection(MachineConnectionString))
                    {
                        conn.Open();

                        using (var command = conn.CreateCommand())
                        {
                            command.CommandText = "DROP DATABASE IF Exists SqlDslTestDb;";
                            command.ExecuteNonQuery();
                        }

                        using (var command = conn.CreateCommand())
                        {
                            command.CommandText = "CREATE DATABASE SqlDslTestDb;";
                            command.ExecuteNonQuery();
                        }
                    }
                },
                () => new Connection());
        }

        private static string MachineConnectionString = "Server=127.0.0.1;Uid=root;Pwd=root;Allow User Variables=True;";

        public static MySqlConnection CreateMySqlConnection() => new MySqlConnection(MachineConnectionString + @"Database=SqlDslTestDb;");

        class Connection : IConnection
        {
            readonly MySqlConnection _connection = CreateMySqlConnection();

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
                }).ToArray());
                
                cmd.ExecuteNonQuery();
            }

            public void Open()
            {
                _connection.Open();
            }
        }
    }
}
