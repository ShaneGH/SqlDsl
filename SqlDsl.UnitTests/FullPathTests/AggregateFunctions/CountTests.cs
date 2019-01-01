using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.AggregateFunctions
{
    [TestFixture]
    public class CountTests : FullPathTestBase
    {
        class JoinedQueryClassLists
        {
            public Person ThePerson { get; set; }
            public List<PersonClass> ThePersonClasses { get; set; }
            public List<Class> TheClasses { get; set; }
            public List<ClassTag> TheClassTags { get; set; }
            public List<Tag> TheTags { get; set; }
        }
        
        class JoinedQueryClassArrays
        {
            public Person ThePerson { get; set; }
            public PersonClass[] ThePersonClasses { get; set; }
            public Class[] TheClasses { get; set; }
            public ClassTag[] TheClassTags { get; set; }
            public Tag[] TheTags { get; set; }
        }
        
        class JoinedQueryClassHashSets
        {
            public Person ThePerson { get; set; }
            public HashSet<PersonClass> ThePersonClasses { get; set; }
            public HashSet<Class> TheClasses { get; set; }
            public HashSet<ClassTag> TheClassTags { get; set; }
            public HashSet<Tag> TheTags { get; set; }
        }

        static Dsl.IQuery<TArg, JoinedQueryClassLists> FullyJoinedQueryLists<TArg>()
        {
            return Sql.Query.Sqlite<TArg, JoinedQueryClassLists>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id);
        }

        static Dsl.IQuery<TArg, JoinedQueryClassArrays> FullyJoinedQueryArrays<TArg>()
        {
            return Sql.Query.Sqlite<TArg, JoinedQueryClassArrays>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id);
        }

        static Dsl.IQuery<TArg, JoinedQueryClassHashSets> FullyJoinedQueryHashSets<TArg>()
        {
            return Sql.Query.Sqlite<TArg, JoinedQueryClassHashSets>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id);
        }

        [Test]
        public async Task CountAndGroup_CountOnTable()
        {
            // arrange
            // act
            var data = await FullyJoinedQueryLists<object>()
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Count()
                })
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 2
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 1
                }
            }, data);
        }

        [Test]
        public async Task CountAndGroup_WithIEnumerableCount()
        {
            // arrange
            // act
            var data = await FullyJoinedQueryLists<object>()
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Select(x => x.Id).Count()
                })
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 2
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 1
                }
            }, data);
        }

        [Test]
        public async Task CountAndGroup_WithListCount()
        {
            // arrange
            // act
            var data = await FullyJoinedQueryLists<object>()
                .Map(p => new 
                {
                    classes = p.TheClasses.Count
                })
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] { new { classes = 2 }, new { classes = 1 } }, data);
        }

        [Test]
        public async Task CountAndGroup_WithHashCount()
        {
            // arrange
            // act
            var data = await FullyJoinedQueryHashSets<object>()
                .Map(p => new 
                {
                    classes = p.TheClasses.Count
                })
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] { new { classes = 2 }, new { classes = 1 } }, data);
        }

        [Test]
        public async Task CountAndGroup_WithArrayLength()
        {
            // arrange
            // act
            var data = await FullyJoinedQueryArrays<object>()
                .Map(p => new 
                {
                    classes = p.TheClasses.Length
                })
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] { new { classes = 2 }, new { classes = 1 } }, data);
        }
    }
}