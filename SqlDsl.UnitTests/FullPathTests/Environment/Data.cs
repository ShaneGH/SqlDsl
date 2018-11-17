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
    static class Data
    {
        public static readonly People People = new People();
        public static readonly PeoplesData PeoplesData = new PeoplesData();
        public static readonly Classes Classes = new Classes();
        public static readonly Tags Tags = new Tags();
        public static readonly PersonClasses PersonClasses = new PersonClasses();
        public static readonly ClassTags ClassTags = new ClassTags();
        public static readonly Purchases Purchases = new Purchases();
    }

    public static class InitData
    {
        static bool _Init = false;
        static readonly object Lock = new object();

        public static void EnsureInit()
        {
            lock (Lock)
            {
                if (_Init) return;
                Init();
                _Init = true;
            }
        }

        public static string GetDbLocation()
        {
            var locationParts = new Regex(@"\\|/").Split(typeof(InitData).Assembly.Location).ToArray();
            return locationParts.Take(locationParts.Length - 1).JoinString(@"\") + @"\data.db";
        }

        public static SqliteConnection CreateConnection() => new SqliteConnection($@"Data Source={GetDbLocation()}");

        static void Init() =>
            InitWithData(
                people: Data.People,
                peoplesData: Data.PeoplesData,
                classes: Data.Classes,
                tags: Data.Tags,
                personClasses: Data.PersonClasses,
                classTags: Data.ClassTags,
                purchases: Data.Purchases);

        public static void InitWithData(
            IEnumerable<Person> people = null,
            IEnumerable<PersonsData> peoplesData = null,
            IEnumerable<PersonClass> personClasses = null,
            IEnumerable<Class> classes = null,
            IEnumerable<ClassTag> classTags = null,
            IEnumerable<Tag> tags = null,
            IEnumerable<Purchase> purchases = null)
        {
            var location = GetDbLocation();
            if (File.Exists(location)) File.Delete(location);

            using (var conn = CreateConnection())
            {   
                conn.Open();
                var paramaters = new List<object>();
                var sql = new[]
                {
                    PopulatedTableSql(people.OrEmpty(), paramaters),
                    PopulatedTableSql(peoplesData.OrEmpty(), paramaters),
                    PopulatedTableSql(classes.OrEmpty(), paramaters),
                    PopulatedTableSql(tags.OrEmpty(), paramaters),
                    PopulatedTableSql(personClasses.OrEmpty(), paramaters),
                    PopulatedTableSql(classTags.OrEmpty(), paramaters),
                    PopulatedTableSql(purchases.OrEmpty(), paramaters)
                }.JoinString("\n");
                
                Console.WriteLine($" * Adding data");

                var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                cmd.Parameters.AddRange(paramaters.Select((p, i) => 
                {
                    var param = cmd.CreateParameter();
                    param.ParameterName = "p" + i;
                    param.Value = p == null ? DBNull.Value : p;
                    return param;
                }));
                
                cmd.ExecuteNonQuery();

                Console.WriteLine($" * Data added");
            }
        }

        public static string PopulatedTableSql<T>(IEnumerable<T> entities, List<object> paramaters)
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

        static string GetColType(Type t)
        {
            if (t == typeof(string)) return "TEXT";
            if (t == typeof(int)) return "INTEGER";
            if (t == typeof(int?)) return "INTEGER";
            if (t == typeof(long)) return "INTEGER";
            if (t == typeof(long?)) return "INTEGER";
            if (t == typeof(float)) return "REAL";
            if (t == typeof(byte[])) return "BLOB";
            if (t.IsEnum) return "INTEGER";

            throw new NotSupportedException($"Invalid database data type: {t}");
        }

        static string GetPrimaryKey(string name) => name.ToLowerInvariant() == "id" ? " PRIMARY KEY" : "";

        static string SqlValue(object val, Type t, List<object> parameters)
        {
            if (t == typeof(int)) return val.ToString();
            if (t == typeof(int?)) return val == null ? "NULL" : val.ToString();
            if (t == typeof(long)) return val.ToString();
            if (t == typeof(long?)) return val == null ? "NULL" : val.ToString();
            if (t == typeof(float)) return val.ToString();
            if (t == typeof(string)) return val == null ? "NULL" : ("'" + val.ToString() + "'");
            if (t == typeof(byte[])) 
            {
                parameters.Add(val);
                return "@p" + (parameters.Count - 1);
            }

            if (t.IsEnum) return ((int)val).ToString();

            throw new NotSupportedException($"Unsupported sql value {val}, {t}");
        }

        static string GetNullable(Type t) => t.IsClass || t.FullName.StartsWith("System.Nullable`1[") ? " NULL" : " NOT NULL";
    }

    class People : IEnumerable<Person>
    {
        public readonly Person John = new Person
        {
            Id = 1,
            Name = "John",
            Gender = Gender.Male
        };
        
        public readonly Person Mary = new Person
        {
            Id = 2,
            Name = "Mary",
            Gender = Gender.Female
        };

        public IEnumerator<Person> GetEnumerator() => (new [] { John, Mary } as IEnumerable<Person>).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    class PeoplesData : IEnumerable<PersonsData>
    {
        public readonly PersonsData JohnsData = new PersonsData
        {
            PersonId = new People().John.Id,
            Data = new byte[] { 1, 2, 3 }
        };

        public readonly PersonsData MarysData = new PersonsData
        {
            PersonId = new People().Mary.Id,
            Data = new byte[] { 4, 5, 6 }
        };

        public IEnumerator<PersonsData> GetEnumerator() => (new [] { JohnsData, MarysData } as IEnumerable<PersonsData>).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
    
    class Classes : IEnumerable<Class>
    {
        public readonly Class Tennis = new Class
        {
            Id = 3,
            Name = "Tennis"
        };
        
        public readonly Class Archery = new Class
        {
            Id = 4,
            Name = "Archery"
        };

        public IEnumerator<Class> GetEnumerator() => (new [] { Tennis, Archery } as IEnumerable<Class>).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
    
    class Tags : IEnumerable<Tag>
    {
        public readonly Tag Sport = new Tag
        {
            Id = 5,
            Name = "Sport"
        };
        
        public readonly Tag BallSport = new Tag
        {
            Id = 6,
            Name = "Ball sport"
        };

        public IEnumerator<Tag> GetEnumerator() => (new [] { Sport, BallSport } as IEnumerable<Tag>).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
    
    class PersonClasses : IEnumerable<PersonClass>
    {
        public readonly PersonClass JohnArchery = new PersonClass
        {
            PersonId = new People().John.Id,
            ClassId = new Classes().Archery.Id
        };
        
        public readonly PersonClass JohnTennis = new PersonClass
        {
            PersonId = new People().John.Id,
            ClassId = new Classes().Tennis.Id
        };
        
        public readonly PersonClass MaryTennis = new PersonClass
        {
            PersonId = new People().Mary.Id,
            ClassId = new Classes().Tennis.Id
        };

        public IEnumerator<PersonClass> GetEnumerator() => (new [] { JohnArchery, JohnTennis, MaryTennis } as IEnumerable<PersonClass>).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
    
    class ClassTags : IEnumerable<ClassTag>
    {
        public readonly ClassTag ArcherySport = new ClassTag
        {
            TagId = new Tags().Sport.Id,
            ClassId = new Classes().Archery.Id
        };
        
        public readonly ClassTag TennisSport = new ClassTag
        {
            TagId = new Tags().Sport.Id,
            ClassId = new Classes().Tennis.Id
        };
        
        public readonly ClassTag TennisBallSport = new ClassTag
        {
            TagId = new Tags().BallSport.Id,
            ClassId = new Classes().Tennis.Id
        };

        public IEnumerator<ClassTag> GetEnumerator() => (new [] { ArcherySport, TennisSport, TennisBallSport } as IEnumerable<ClassTag>).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
    
    class Purchases : IEnumerable<Purchase>
    {
        public readonly Purchase JohnPurchasedHimselfShoes = new Purchase
        {
            Id = 7,
            Amount = 100,
            PersonId = new People().John.Id,
            PurchaedForPersonId = new People().John.Id,
            ClassId = null
        };

        public readonly Purchase JohnPurchasedHimselfTennis = new Purchase
        {
            Id = 8,
            Amount = 200,
            PersonId = new People().John.Id,
            PurchaedForPersonId = new People().John.Id,
            ClassId = new Classes().Tennis.Id
        };

        public readonly Purchase MaryPurchasedHerselfTennis = new Purchase
        {
            Id = 9,
            Amount = 300,
            PersonId = new People().Mary.Id,
            PurchaedForPersonId = new People().Mary.Id,
            ClassId = new Classes().Tennis.Id
        };

        public readonly Purchase MaryPurchasedJohnArchery = new Purchase
        {
            Id = 10,
            Amount = 400,
            PersonId = new People().Mary.Id,
            PurchaedForPersonId = new People().John.Id,
            ClassId = new Classes().Archery.Id
        };

        public IEnumerator<Purchase> GetEnumerator() => (new [] 
        { 
            JohnPurchasedHimselfShoes,
            JohnPurchasedHimselfTennis,
            MaryPurchasedHerselfTennis,
            MaryPurchasedJohnArchery
        } as IEnumerable<Purchase>).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
