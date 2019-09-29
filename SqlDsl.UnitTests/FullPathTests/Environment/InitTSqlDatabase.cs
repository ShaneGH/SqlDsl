using System;
using System.Collections.Generic;
using System.Linq;
using SqlDsl.UnitTests.Utils;
using SqlDsl.TSql;
using System.Data.SqlClient;

namespace SqlDsl.UnitTests.FullPathTests.Environment
{
    public class InitTSqlDatabase : InitDatabaseBase
    {
        public InitTSqlDatabase(
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
                new TSqlSyntax(),
                new DataTypes
                {
                    String =  "TEXT",
                    DateTime = "DATETIME",
                    DateTime_Null = "DATETIME",
                    Bool = "BIT",
                    Bool_Null = "BIT",
                    Int = "INT",
                    Int_Null = "INT",
                    Long = "BIGINT",
                    Long_Null = "BIGINT",
                    Float = "FLOAT",
                    Float_Null = "FLOAT",
                    ByteArray = "VARBINARY(1024)",
                    Byte = "INT",
                    Byte_Null = "INT",
                    SByte = "INT",
                    SByte_Null = "INT",
                    Short = "INT",
                    Short_Null = "INT",
                    UShort = "INT",
                    UShort_Null = "INT",
                    UInt = "INT",
                    UInt_Null = "INT",
                    ULong = "INT",
                    ULong_Null = "INT",
                    Double = "INT",
                    Double_Null = "INT",
                    Decimal = "INT",
                    Decimal_Null = "INT",
                    Guid = "UNIQUEIDENTIFIER",
                    Guid_Null = "UNIQUEIDENTIFIER",
                    Enum = "SMALLINT",
                    Enum_Null = "SMALLINT"
                },
                (Settings settings) => 
                {
                    using (var conn = new SqlConnection(settings.TSqlConnectionString))
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

        public static SqlConnection CreateTSqlConnection(Settings settings) => new SqlConnection(settings.TSqlConnectionString + @"Database=SqlDslTestDb;");

        class Connection : IConnection
        {
            readonly SqlConnection _connection;

            public Connection(Settings settings)
            {
                _connection = CreateTSqlConnection(settings);
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
