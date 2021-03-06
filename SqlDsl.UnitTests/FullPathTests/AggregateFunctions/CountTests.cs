using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.AggregateFunctions
{
    [SqlTestAttribute(SqlType.TSql)]
    [SqlTestAttribute(SqlType.MySql)]
    [SqlTestAttribute(SqlType.Sqlite)]
    public class CountTests : FullPathTestBase
    {
        public CountTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
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

        Dsl.IQuery<JoinedQueryClassLists> FullyJoinedQueryLists(bool strictJoins = true)
        {
            return Query<JoinedQueryClassLists>(strictJoins )
                .From<Person>(x => x.ThePerson)
                .InnerJoinMany<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoinMany<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoinMany<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .InnerJoinMany<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id);
        }

        Dsl.IQuery<JoinedQueryClassArrays> FullyJoinedQueryArrays(bool strictJoins = true)
        {
            return Query<JoinedQueryClassArrays>(strictJoins)
                .From<Person>(x => x.ThePerson)
                .InnerJoinMany<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoinMany<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoinMany<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .InnerJoinMany<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id);
        }

        Dsl.IQuery<JoinedQueryClassHashSets> FullyJoinedQueryHashSets(bool strictJoins = true)
        {
            return Query<JoinedQueryClassHashSets>(strictJoins)
                .From<Person>(x => x.ThePerson)
                .InnerJoinMany<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoinMany<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoinMany<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .InnerJoinMany<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id);
        }

        [Test]
        public async Task CountAndGroup_CountOnTable()
        {
            // arrange
            // act
            var data = await FullyJoinedQueryLists(false)
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Count()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

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
            var data = await FullyJoinedQueryLists(false)
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Select(x => x.Id).Count()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

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
            var data = await FullyJoinedQueryLists(false)
                .Map(p => new 
                {
                    classes = p.TheClasses.Count
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] { new { classes = 2 }, new { classes = 1 } }, data);
        }

        [Test]
        public async Task CountAndGroup_WithHashCount()
        {
            // arrange
            // act
            var data = await FullyJoinedQueryHashSets(false)
                .Map(p => new 
                {
                    classes = p.TheClasses.Count
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] { new { classes = 2 }, new { classes = 1 } }, data);
        }

        [Test]
        public async Task CountAndGroup_WithArrayLength()
        {
            // arrange
            // act
            var data = await FullyJoinedQueryArrays(false)
                .Map(p => new 
                {
                    classes = p.TheClasses.Length
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] { new { classes = 2 }, new { classes = 1 } }, data);
        }
    }
}