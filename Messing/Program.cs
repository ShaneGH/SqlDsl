using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;
using SqlDsl.Sqlite;
using SqlDsl.Utils;

namespace SqlDsl
{
    abstract class EqComparer
    {        
        public static bool operator !=(EqComparer a, EqComparer b) => !(a == b);

        public static bool operator ==(EqComparer a, EqComparer b)
        {
            return Object.ReferenceEquals(null, a) ?
                Object.ReferenceEquals(null, b) :
                a.Equals(b);
        }
    }
    
    class Person : EqComparer
    {
        public int Id { get; set; }
        public string Name { get; set; }

        public override int GetHashCode() => $"{Id}.{Name}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as Person;
            return person != null && person.Id == Id && person.Name == Name;
        }
    }
    
    class PersonClass : EqComparer
    {
        public int PersonId { get; set; }
        public int ClassId { get; set; }

        public override int GetHashCode() => $"{PersonId}.{ClassId}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as PersonClass;
            return person != null && person.PersonId == PersonId && person.ClassId == ClassId;
        }
    }
    
    class Class : EqComparer
    {
        public int Id { get; set; }
        public string Name { get; set; }

        public override int GetHashCode() => $"{Id}.{Name}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as Class;
            return person != null && person.Id == Id && person.Name == Name;
        }
    }
    
    class Tag : EqComparer
    {
        public int Id { get; set; }
        public string Name { get; set; }

        public override int GetHashCode() => $"{Id}.{Name}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as Tag;
            return person != null && person.Id == Id && person.Name == Name;
        }
    }
    
    class ClassTag : EqComparer
    {
        public int ClassId { get; set; }
        public int TagId { get; set; }

        public override int GetHashCode() => $"{ClassId}.{TagId}".GetHashCode();
        public override bool Equals(object p)
        {
            var person = p as ClassTag;
            return person != null && person.ClassId == ClassId && person.TagId == TagId;
        }
    }

    class QueryClass
    {
        public Person Person { get; set; }
        public IEnumerable<PersonClass> PersonClasses { get; set; }
        public IEnumerable<Class> Classes { get; set; }
        public IEnumerable<ClassTag> ClassTags { get; set; }
        public IEnumerable<Tag> Tags { get; set; }
    }

    class ResultClass : Object
    {
        public string PersonName { get; set; }
        // public IEnumerable<string> ClassNames { get; set; }
        // public IEnumerable<string> ClassTags { get; set; }
    }

    class Program
    {
        static ISqlBuilder<ResultClass> Q()
        {
            return Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)

                .LeftJoin(nameof(PersonClass), result => result.PersonClasses)
                .On((result, _class) => _class.PersonId == result.Person.Id)

                .InnerJoin(nameof(Class), result => result.Classes)
                .On((result, _class) => _class.Id == Sql.One(result.PersonClasses).ClassId)

                .InnerJoin(nameof(ClassTag), result => result.ClassTags)
                .On((result, classTag) => classTag.ClassId == Sql.One(result.Classes).Id)

                .InnerJoin(nameof(Tag), result => result.Tags)
                .On((result, tag) => tag.Id == Sql.One(result.ClassTags).TagId)

                .Where(result => result.Person.Id == 1)
                .Map(x => new ResultClass
                {
                    PersonName = x.Person.Name,
                    //ClassNames = x.Classes.Select(c => c.Name),
                    //ClassTags = x.Tags.Select(c => c.Name)
                })
                ;
        }

        static void Main(string[] args)
        {
            Setup();
            using (var conn = new SqliteConnection(@"Data Source=C:\Dev\SqlDsl\Messing\data.db"))
            {
                conn.Open();
                var cmd = Q();

                var sql = cmd.ToSql();
                Console.WriteLine();
                Console.WriteLine(sql.sql);
                Console.WriteLine();

               // ExecuteSql(conn, sql.sql, sql.paramaters).Wait();


                var data = Q()
                    .ExecuteAsync(new SqliteExecutor(conn));

               Console.WriteLine(JsonConvert.SerializeObject(data.Result, Formatting.Indented));

            //Time(() => Q());
            }
        }

        static void Time(Action f)
        {
            var total = 100;
            var time = DateTime.Now;
            for (var i = 0; i < total; i++)
                f();

            Console.WriteLine((DateTime.Now - time)/total);        
        }

        static async Task ExecuteSql(SqliteConnection connection, string sql, IEnumerable<object> paramaters)
        {
            var rows = await (await new SqliteExecutor(connection)
                .ExecuteAsync(sql, paramaters))
                .GetRowsAsync();

            if (!rows.Any())
            {
                Console.WriteLine("No rows");
                return;
            }

            Console.WriteLine(rows.First().Select(cell => cell.Item1).JoinString(" "));
            rows
                .ToList()
                .ForEach(row => Console.WriteLine(row.Select(cell => cell.Item2).JoinString(" ")));
        }

        static void Setup()
        {
            var file = @"C:\Dev\SqlDsl\Messing\data.db";
            if (File.Exists(file))
                File.Delete(file);

            File.Create(file).Dispose();

            using (var conn = new SqliteConnection(@"Data Source=C:\Dev\SqlDsl\Messing\data.db"))
            {   
                conn.Open();

                var commandText = new[]
                {
                    $"CREATE TABLE [{nameof(Person)}] (",
                    $"  [{nameof(Person.Id)}]   INTEGER PRIMARY KEY,",
                    $"  [{nameof(Person.Name)}] TEXT NOT NULL",
                    $");",
                    "",
                    $"CREATE TABLE [{nameof(PersonClass)}] (",
                    $"  [{nameof(PersonClass.PersonId)}]    INTEGER NOT NULL,",
                    $"  [{nameof(PersonClass.ClassId)}]     INTEGER NOT NULL",
                    $");",
                    "",
                    $"CREATE TABLE [{nameof(Class)}] (",
                    $"  [{nameof(Class.Id)}]   INTEGER PRIMARY KEY,",
                    $"  [{nameof(Class.Name)}] TEXT NOT NULL",
                    $");",
                    "",
                    $"CREATE TABLE [{nameof(Tag)}] (",
                    $"  [{nameof(Tag.Id)}]   INTEGER PRIMARY KEY,",
                    $"  [{nameof(Tag.Name)}] TEXT NOT NULL",
                    $");",
                    "",
                    $"CREATE TABLE [{nameof(ClassTag)}] (",
                    $"  [{nameof(ClassTag.ClassId)}]    INTEGER NOT NULL,",
                    $"  [{nameof(ClassTag.TagId)}]      INTEGER NOT NULL",
                    $");"
                };

                var cmd = conn.CreateCommand();
                Console.WriteLine($" * Adding table(s)");
                cmd.CommandText = string.Join(Environment.NewLine, commandText);
                cmd.ExecuteNonQuery();
                Console.WriteLine($" * Table(s) added");

                commandText = new[]
                {
                    $"INSERT INTO [{nameof(Person)}]",
                    $"VALUES (1, 'John');",
                    $"INSERT INTO [{nameof(Person)}]",
                    $"VALUES (2, 'Mary');",
                    "",
                    $"INSERT INTO [{nameof(Class)}]",
                    $"VALUES (3, 'Tennis');",
                    $"INSERT INTO [{nameof(Class)}]",
                    $"VALUES (4, 'Archery');",
                    "",
                    $"INSERT INTO [{nameof(PersonClass)}]",
                    $"VALUES (1, 3);",
                    $"INSERT INTO [{nameof(PersonClass)}]",
                    $"VALUES (1, 4);",
                    $"INSERT INTO [{nameof(PersonClass)}]",
                    $"VALUES (2, 4);",
                    "",
                    $"INSERT INTO [{nameof(Tag)}]",
                    $"VALUES (5, 'Sport');",
                    $"INSERT INTO [{nameof(Tag)}]",
                    $"VALUES (6, 'Ball sport');",
                    "",
                    $"INSERT INTO [{nameof(ClassTag)}]",
                    $"VALUES (3, 5);",
                    $"INSERT INTO [{nameof(ClassTag)}]",
                    $"VALUES (3, 6);",
                    $"INSERT INTO [{nameof(ClassTag)}]",
                    $"VALUES (4, 5);",
                    "",
                };
                
                cmd = conn.CreateCommand();
                Console.WriteLine($" * Adding data");
                cmd.CommandText = string.Join(Environment.NewLine, commandText);
                cmd.ExecuteNonQuery();
                Console.WriteLine($" * data added");
            }
        }
    }
}
