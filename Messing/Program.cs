using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;
using SqlDsl.Dsl;
using SqlDsl.Sqlite;
using SqlDsl.UnitTests.FullPathTests;
using SqlDsl.UnitTests.FullPathTests.Environment;
using SqlDsl.Utils;

namespace SqlDsl
{
    public class Program
    {
        static (List<Person>, List<PersonClass>, List<Class>, List<ClassTag>, List<Tag>) BuildData()
        {
            var cs = new List<Class>
            {
                new Class { Id = 1, Name = "Tesselation" },
                new Class { Id = 2, Name = "Underpants knitting" },
                new Class { Id = 3, Name = "Kickboxing" },
                new Class { Id = 4, Name = "Pro gaming" }
            };

            var tags = new List<Tag>
            {
                new Tag { Id = 1, Name = "Is good" },
                new Tag { Id = 2, Name = "Is bad" }
            };

            var cltg = new List<ClassTag>
            {
                new ClassTag
                {
                    ClassId = 1,
                    TagId = 1
                },
                new ClassTag
                {
                    ClassId = 2,
                    TagId = 2
                },
                new ClassTag
                {
                    ClassId = 3,
                    TagId = 1
                },
                new ClassTag
                {
                    ClassId = 3,
                    TagId = 2
                }
            };

            var ps = new List<Person>();
            var pcs = new List<PersonClass>();
            for (var i = 1; i <= 50; i++)
            {
                ps.Add(new Person(i, $"person {i.ToString()}", i %2 == 0 ? Gender.Male : Gender.Female, i %3 == 0));
                foreach (var c in cs)
                {
                    pcs.Add(new PersonClass
                    {
                        PersonId = i,
                        ClassId = c.Id
                    });
                }
            }

            return (ps, pcs, cs, cltg, tags);
        }

        static void Main(string[] args)
        {
            var (p, pc, c, clt, t) = BuildData();
            InitData.InitWithData(p, null, pc, c, clt, t);

            var logger = new FullPathTestBase.TestLogger();
            logger.SupressLogMessages.Add(LogMessages.ExecutingQuery);

            using (var connection = InitData.CreateSqliteConnection())
            {
                connection.Open();
                var executor = new TestExecutor(new SqliteExecutor(connection));

                Sql.Query
                    .Sqlite<(Person person, IEnumerable<PersonClass> personClass, IEnumerable<Class> cls, IEnumerable<ClassTag> classTags, IEnumerable<Tag> tags)>()
                    .From(x => x.person)
                    .InnerJoin(x => x.personClass).On((q, x) => q.person.Id == x.PersonId)
                    .InnerJoin(x => x.cls).On((q, x) => q.personClass.One().ClassId == x.Id)
                    .InnerJoin(x => x.classTags).On((q, x) => q.cls.One().Id == x.ClassId)
                    .InnerJoin(x => x.tags).On((q, x) => q.classTags.One().TagId == x.Id)
                    .Map(x => new 
                    {
                        name = x.person.Name,
                        classes = x.cls
                            .Select(cl => new 
                            {
                                name = cl.Name,
                                tags = x.tags
                                    .Select(z => z.Name)
                                    .ToArray()
                            })
                            .ToArray()
                    })
                    .ToArray(executor);

                var results = Sql.Query
                    .Sqlite<(Person person, IEnumerable<PersonClass> personClass, IEnumerable<Class> cls, IEnumerable<ClassTag> classTags, IEnumerable<Tag> tags)>()
                    .From(x => x.person)
                    .InnerJoin(x => x.personClass).On((q, x) => q.person.Id == x.PersonId)
                    .InnerJoin(x => x.cls).On((q, x) => q.personClass.One().ClassId == x.Id)
                    .InnerJoin(x => x.classTags).On((q, x) => q.cls.One().Id == x.ClassId)
                    .InnerJoin(x => x.tags).On((q, x) => q.classTags.One().TagId == x.Id)
                    .Map(x => new { x.person })
                    .ToArray(executor, logger);

                Console.WriteLine("results count " + results.Count());
                logger.PrintAllLogs();
            }
        }
    }
}
