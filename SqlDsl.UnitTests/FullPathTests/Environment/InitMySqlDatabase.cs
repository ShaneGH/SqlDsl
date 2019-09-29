using System;
using System.Collections.Generic;
using System.Linq;
using MySql.Data.MySqlClient;
using SqlDsl.MySql;
using SqlDsl.UnitTests.Utils;

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
                new MySqlSyntax(MySqlSettings.Default),
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
                    // TODO: saving as BINARY returns array
                    Byte = "INT",
                    Byte_Null = "INT",
                    SByte = "INT",
                    SByte_Null = "INT",
                    Short = "SMALLINT",
                    Short_Null = "SMALLINT",
                    UShort = "SMALLINT UNSIGNED",
                    UShort_Null = "SMALLINT UNSIGNED",
                    UInt = "INT UNSIGNED",
                    UInt_Null = "INT UNSIGNED",
                    ULong = "BIGINT UNSIGNED",
                    ULong_Null = "BIGINT UNSIGNED",
                    Double = "DOUBLE",
                    Double_Null = "DOUBLE",
                    Decimal = "DOUBLE",
                    Decimal_Null = "DOUBLE",
                    Char = "CHAR",
                    Char_Null = "CHAR",
                    Guid = "VARBINARY(16)",
                    Guid_Null = "VARBINARY(16)",
                    Enum = "SMALLINT",
                    Enum_Null = "SMALLINT"
                },
                (Settings settings) => 
                {
                    using (var conn = new MySqlConnection(settings.MySqlConnectionString))
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
                (Settings settings) => new Connection(settings));
        }

        public static MySqlConnection CreateMySqlConnection(Settings settings) => new MySqlConnection(settings.MySqlConnectionString + @"Database=SqlDslTestDb;");

        class Connection : IConnection
        {
            readonly MySqlConnection _connection;

            public Connection(Settings settings)
            {
                _connection = CreateMySqlConnection(settings);
            }

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
