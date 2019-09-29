using System;
using System.Collections.Generic;
using System.Linq;
using SqlDsl.SqlBuilders;
using SqlDsl.UnitTests.Utils;
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
        public string Byte { get; set; }
        public string Byte_Null { get; set; }
        public string SByte { get; set; }
        public string SByte_Null { get; set; }
        public string Short { get; set; }
        public string Short_Null { get; set; }
        public string UShort { get; set; }
        public string UShort_Null { get; set; }
        public string UInt { get; set; }
        public string UInt_Null { get; set; }
        public string ULong { get; set; }
        public string ULong_Null { get; set; }
        public string Double { get; set; }
        public string Double_Null { get; set; }
        public string Decimal { get; set; }
        public string Decimal_Null { get; set; }
        public string Char { get; set; }
        public string Char_Null { get; set; }
        public string Guid { get; set; }
        public string Guid_Null { get; set; }
        public string Enum { get; set; }
        public string Enum_Null { get; set; }
    }

    public class Dependencies
    {
        public readonly DataTypes DataTypes;
        public readonly Action<Settings> CleanupOldDatabase;
        public readonly Func<Settings, IConnection> CreateConnection;
        public readonly ISqlSyntax SqlSyntax;

        public Dependencies(
            ISqlSyntax sqlSyntax,
            DataTypes dataTypes,
            Action<Settings> cleanupOldDatabase,
            Func<Settings, IConnection> createConnection)
        {
            DataTypes = dataTypes;
            CleanupOldDatabase = cleanupOldDatabase;
            CreateConnection = createConnection;
            SqlSyntax = sqlSyntax;
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
        readonly IEnumerable<TestDataTable> TestData;

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
            IEnumerable<DataDump> dataDump,
            IEnumerable<TestDataTable> testData)
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
            TestData = testData;
        }

        public void Execute(Settings settings)
        {
            Dependencies.CleanupOldDatabase(settings);

            using (var conn = Dependencies.CreateConnection(settings))
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
                    PopulatedTableSql(DataDump.OrEmpty(), paramaters),
                    PopulatedTableSql(TestData.OrEmpty(), paramaters)
                }.JoinString("\n");
                
                Console.WriteLine($" * Adding data");

                try 
                {
                    conn.ExecuteCommand(sql, paramaters);
                }
                catch (Exception e)
                {
                    throw new Exception("Error executing query:\n" + sql, e);
                }

                Console.WriteLine($" * Data added");
            }
        }

        string PopulatedTableSql<T>(IEnumerable<T> entities, List<object> paramaters)
        {
            var createSql = new List<string>();
            var insertSql = new List<string>();

            var props = typeof(T)
                .GetProperties()
                .Select(x => new { x.Name, x.PropertyType, Get = (Func<T, object>)(e => x.GetMethod.Invoke(e, new object[0])) })
                .Concat(typeof(T)
                    .GetFields()
                    .Select(x => new { x.Name, PropertyType = x.FieldType, Get = (Func<T, object>)(e => x.GetValue(e)) }))
                .ToList();

            var columns = props
                .Select(prop => $"`{prop.Name}` {GetColType(prop.PropertyType)} {GetPrimaryKey(prop.Name)} {GetNullable(prop.PropertyType)}");

            var data = entities
                .Select(e => props
                    .Select(p => 
                        SqlValue(p.Get(e), p.PropertyType, paramaters)).JoinString(", "))
                .Select(v => $"INSERT INTO {Dependencies.SqlSyntax.WrapTable(typeof(T).Name)}\nVALUES ({v});");

            return new [] 
            {
                $"CREATE TABLE {Dependencies.SqlSyntax.WrapTable(typeof(T).Name)} (",
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
            if (t == typeof(byte)) return Dependencies.DataTypes.Byte;
            if (t == typeof(byte?)) return Dependencies.DataTypes.Byte_Null;
            if (t == typeof(sbyte)) return Dependencies.DataTypes.SByte;
            if (t == typeof(sbyte?)) return Dependencies.DataTypes.SByte_Null;
            if (t == typeof(short)) return Dependencies.DataTypes.Short;
            if (t == typeof(short?)) return Dependencies.DataTypes.Short_Null;
            if (t == typeof(ushort)) return Dependencies.DataTypes.UShort;
            if (t == typeof(ushort?)) return Dependencies.DataTypes.UShort_Null;
            if (t == typeof(uint)) return Dependencies.DataTypes.UInt;
            if (t == typeof(uint?)) return Dependencies.DataTypes.UInt_Null;
            if (t == typeof(ulong)) return Dependencies.DataTypes.ULong;
            if (t == typeof(ulong?)) return Dependencies.DataTypes.ULong_Null;
            if (t == typeof(double)) return Dependencies.DataTypes.Double;
            if (t == typeof(double?)) return Dependencies.DataTypes.Double_Null;
            if (t == typeof(decimal)) return Dependencies.DataTypes.Decimal;
            if (t == typeof(decimal?)) return Dependencies.DataTypes.Decimal_Null;
            if (t == typeof(char)) return Dependencies.DataTypes.Char;
            if (t == typeof(char?)) return Dependencies.DataTypes.Char_Null;
            if (t == typeof(Guid)) return Dependencies.DataTypes.Guid;
            if (t == typeof(Guid?)) return Dependencies.DataTypes.Guid_Null;
            if (t == typeof(IEnumerable<char>)) return Dependencies.DataTypes.String;
            if (t == typeof(List<char>)) return Dependencies.DataTypes.String;
            if (t == typeof(char[])) return Dependencies.DataTypes.String;
            if (t == typeof(IEnumerable<byte>)) return Dependencies.DataTypes.ByteArray;
            if (t == typeof(List<byte>)) return Dependencies.DataTypes.ByteArray;
            if (t == typeof(byte[])) return Dependencies.DataTypes.ByteArray;
            if (t.IsEnum) return Dependencies.DataTypes.Enum;
            if (TryGetNullableType(t)?.IsEnum ?? false) return Dependencies.DataTypes.Enum_Null;

            throw new NotSupportedException($"Invalid database data type: {t}");
        }

        Type TryGetNullableType(Type t)
        {
            if (t.IsGenericType &&
                t.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                return t.GetGenericArguments()[0];
            }

            return null;
        }

        string GetPrimaryKey(string name) => name.ToLowerInvariant() == "id" ? " PRIMARY KEY" : "";

        string SqlValue(object val, Type t, List<object> parameters)
        {
            if (val == null) return "NULL";

            if (t == typeof(IEnumerable<char>) ||
                t == typeof(char[]) || 
                t == typeof(List<char>))
            {
                return SqlValue(new string((val as IEnumerable<char>).ToArray()), typeof(string), parameters);
            }

            if (t == typeof(char) || t == typeof(char?))
            {
                return SqlValue(val?.ToString(), typeof(string), parameters);
            }

            if (t == typeof(IEnumerable<byte>) ||
                t == typeof(List<byte>))
            {
                return SqlValue((val as IEnumerable<byte>).ToArray(), typeof(byte[]), parameters);
            }

            if (t == typeof(Guid?))
            {
                // value will not be null (see above)
                return SqlValue(((Guid?)val).Value, typeof(Guid), parameters);
            }

            if (t == typeof(Guid))
            {
                return SqlValue(((Guid)val).ToByteArray(), typeof(byte[]), parameters);
            }

            parameters.Add(val);
            return $"@p{parameters.Count - 1}";
        }

        string GetNullable(Type t) => t.IsClass || t.IsInterface || t.FullName.StartsWith("System.Nullable`1[") ? " NULL" : " NOT NULL";
    }
}
