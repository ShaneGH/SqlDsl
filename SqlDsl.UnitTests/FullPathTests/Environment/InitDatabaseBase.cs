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
using SqlDsl.Utils;

namespace SqlDsl.UnitTests.FullPathTests.Environment
{
    public class DataTypes 
    {
        public string String { get; set; }
        public string DateTime { get; set; }
        public string DateTime_Null { get; set; }
        public string Bool { get; set; }
        public string Bool_Null { get; set; }
        public string Int { get; set; }
        public string Int_Null { get; set; }
        public string Long { get; set; }
        public string Long_Null { get; set; }
        public string Float { get; set; }
        public string Float_Null { get; set; }
        public string ByteArray { get; set; }
        public string Enum { get; set; }
        
        public Func<string, List<object>, string> GetString { get; set; }
        public Func<DateTime, List<object>, string> GetDateTime { get; set; }
        public Func<DateTime?, List<object>, string> GetDateTime_Null { get; set; }
        public Func<bool, List<object>, string> GetBool { get; set; }
        public Func<bool?, List<object>, string> GetBool_Null { get; set; }
        public Func<int, List<object>, string> GetInt { get; set; }
        public Func<int?, List<object>, string> GetInt_Null { get; set; }
        public Func<long, List<object>, string> GetLong { get; set; }
        public Func<long?, List<object>, string> GetLong_Null { get; set; }
        public Func<float, List<object>, string> GetFloat { get; set; }
        public Func<float?, List<object>, string> GetFloat_Null { get; set; }
        public Func<byte[], List<object>, string> GetByteArray { get; set; }
        public Func<int, List<object>, string> GetEnum { get; set; }
    }

    public class Dependencies
    {
        public readonly DataTypes DataTypes;
        public readonly Action CleanupOldDatabase;
        public readonly Func<IConnection> CreateConnection;

        public Dependencies(
            DataTypes dataTypes,
            Action cleanupOldDatabase,
            Func<IConnection> createConnection)
        {
            DataTypes = dataTypes;
            CleanupOldDatabase = cleanupOldDatabase;
            CreateConnection = createConnection;
        }
    }

    public interface IConnection : IDisposable
    {
        void Open();
        void ExecuteCommand(string sql, IEnumerable<object> parameters);
    }

    public abstract class InitDatabaseBase
    {
        readonly Dependencies Dependencies;
        readonly IEnumerable<Person> People;
        readonly IEnumerable<PersonsData> PeoplesData;
        readonly IEnumerable<PersonClass> PersonClasses;
        readonly IEnumerable<Class> Classes;
        readonly IEnumerable<ClassTag> ClassTags;
        readonly IEnumerable<Tag> Tags;
        readonly IEnumerable<Purchase> Purchases;
        readonly IEnumerable<TableWithOneRowAndOneColumn> TablesWithOneColumn;
        readonly IEnumerable<DataDump> DataDump;

        public InitDatabaseBase(
            Dependencies dependencies,
            IEnumerable<Person> people,
            IEnumerable<PersonsData> peoplesData,
            IEnumerable<PersonClass> personClasses,
            IEnumerable<Class> classes,
            IEnumerable<ClassTag> classTags,
            IEnumerable<Tag> tags,
            IEnumerable<Purchase> purchases,
            IEnumerable<TableWithOneRowAndOneColumn> tablesWithOneColumn,
            IEnumerable<DataDump> dataDump)
        {
            Dependencies = dependencies;
            People = people;
            PeoplesData = peoplesData;
            PersonClasses = personClasses;
            Classes = classes;
            ClassTags = classTags;
            Tags = tags;
            Purchases = purchases;
            TablesWithOneColumn = tablesWithOneColumn;
            DataDump = dataDump;
        }

        public void Execute()
        {
            Dependencies.CleanupOldDatabase();

            using (var conn = Dependencies.CreateConnection())
            {   
                conn.Open();
                var paramaters = new List<object>();
                var sql = new[]
                {
                    PopulatedTableSql(People.OrEmpty(), paramaters),
                    PopulatedTableSql(PeoplesData.OrEmpty(), paramaters),
                    PopulatedTableSql(Classes.OrEmpty(), paramaters),
                    PopulatedTableSql(Tags.OrEmpty(), paramaters),
                    PopulatedTableSql(PersonClasses.OrEmpty(), paramaters),
                    PopulatedTableSql(ClassTags.OrEmpty(), paramaters),
                    PopulatedTableSql(Purchases.OrEmpty(), paramaters),
                    PopulatedTableSql(TablesWithOneColumn.OrEmpty(), paramaters),
                    PopulatedTableSql(DataDump.OrEmpty(), paramaters)
                }.JoinString("\n");
                
                Console.WriteLine($" * Adding data");

                conn.ExecuteCommand(sql, paramaters);

                Console.WriteLine($" * Data added");
            }
        }

        string PopulatedTableSql<T>(IEnumerable<T> entities, List<object> paramaters)
        {
            var createSql = new List<string>();
            var insertSql = new List<string>();

            var props = typeof(T)
                .GetProperties()
                .ToList();

            var columns = props
                .Select(prop => $"{prop.Name} {GetColType(prop.PropertyType)} {GetPrimaryKey(prop.Name)} {GetNullable(prop.PropertyType)}");

            var data = entities
                .Select(e => props
                    .Select(p => 
                        SqlValue(p.GetMethod.Invoke(e, new object[0]), p.PropertyType, paramaters)).JoinString(", "))
                .Select(v => $"INSERT INTO [{typeof(T).Name}]\nVALUES ({v});");

            return new [] 
            {
                $"CREATE TABLE [{typeof(T).Name}] (",
                columns.JoinString(",\n"),
                ");",
                data.JoinString("\n")
            }.JoinString("\n");
        }

        string GetColType(Type t)
        {
            if (t == typeof(string)) return Dependencies.DataTypes.String;
            if (t == typeof(DateTime)) return Dependencies.DataTypes.DateTime;
            if (t == typeof(DateTime?)) return Dependencies.DataTypes.DateTime_Null;
            if (t == typeof(bool)) return Dependencies.DataTypes.Bool;
            if (t == typeof(bool?)) return Dependencies.DataTypes.Bool_Null;
            if (t == typeof(int)) return Dependencies.DataTypes.Int;
            if (t == typeof(int?)) return Dependencies.DataTypes.Int_Null;
            if (t == typeof(long)) return Dependencies.DataTypes.Long;
            if (t == typeof(long?)) return Dependencies.DataTypes.Long_Null;
            if (t == typeof(float)) return Dependencies.DataTypes.Float;
            if (t == typeof(float?)) return Dependencies.DataTypes.Float_Null;
            if (t == typeof(byte[])) return Dependencies.DataTypes.ByteArray;
            if (t.IsEnum) return Dependencies.DataTypes.Enum;

            throw new NotSupportedException($"Invalid database data type: {t}");
        }

        string GetPrimaryKey(string name) => name.ToLowerInvariant() == "id" ? " PRIMARY KEY" : "";

        string SqlValue(object val, Type t, List<object> parameters)
        {
            if (t == typeof(string)) return Dependencies.DataTypes.GetString((string)val, parameters);
            if (t == typeof(DateTime)) return Dependencies.DataTypes.GetDateTime((DateTime)val, parameters);
            if (t == typeof(DateTime?)) return Dependencies.DataTypes.GetDateTime_Null((DateTime?)val, parameters);
            if (t == typeof(bool)) return Dependencies.DataTypes.GetBool((bool)val, parameters);
            if (t == typeof(bool?)) return Dependencies.DataTypes.GetBool_Null((bool?)val, parameters);
            if (t == typeof(int)) return Dependencies.DataTypes.GetInt((int)val, parameters);
            if (t == typeof(int?)) return Dependencies.DataTypes.GetInt_Null((int?)val, parameters);
            if (t == typeof(long)) return Dependencies.DataTypes.GetLong((long)val, parameters);
            if (t == typeof(long?)) return Dependencies.DataTypes.GetLong_Null((long?)val, parameters);
            if (t == typeof(float)) return Dependencies.DataTypes.GetFloat((float)val, parameters);
            if (t == typeof(float?)) return Dependencies.DataTypes.GetFloat_Null((float?)val, parameters);
            if (t == typeof(byte[])) return Dependencies.DataTypes.GetByteArray((byte[])val, parameters);
            if (t.IsEnum) return Dependencies.DataTypes.GetEnum((int)val, parameters);

            throw new NotSupportedException($"Unsupported sql value {val}, {t}");
        }

        string GetNullable(Type t) => t.IsClass || t.FullName.StartsWith("System.Nullable`1[") ? " NULL" : " NOT NULL";
    }
}
