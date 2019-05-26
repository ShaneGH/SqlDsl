using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.Joins
{
    [SqlTestAttribute(SqlType.MySql)]
    [SqlTestAttribute(SqlType.Sqlite)]
    public class JoinTests : FullPathTestBase
    {
        public JoinTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        public class QueryClass
        {
            public Person ThePerson { get; set; }
            public IEnumerable<PersonClass> ThePersonClasses { get; set; }
            public IEnumerable<ClassTag> TheClassTags { get; set; }
        }

        public static void AssertSelect1SimpleJoin(IEnumerable<QueryClass> data)
        {
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).ThePerson);

            Assert.AreEqual(2, data.First().ThePersonClasses.Count());
            Assert.Contains(Data.PersonClasses.JohnTennis, data.First().ThePersonClasses.ToList());
            Assert.Contains(Data.PersonClasses.JohnArchery, data.First().ThePersonClasses.ToList());

            Assert.AreEqual(1, data.ElementAt(1).ThePersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.ElementAt(1).ThePersonClasses.ElementAt(0));
        }

        [Test]
        public async Task Select1SimpleInnerJoin()
        {
            // arrange
            // act
            var data = await Query<QueryClass>()
                .From(result => result.ThePerson)
                .InnerJoin<PersonClass>(result => result.ThePersonClasses)
                    .On((q, c) => q.ThePerson.Id == c.PersonId)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            AssertSelect1SimpleJoin(data);
        }

        [Test]
        public void With1Join_MissingConnectingJoin_ThrowsError()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(InvalidOperationException), () =>
                Query<QueryClass>()
                    .From<Person>(x => x.ThePerson)
                    .InnerJoin<ClassTag>(q => q.TheClassTags)
                        .On((q, ct) => q.ThePersonClasses.One().ClassId == ct.ClassId)
                    .ToIEnumerableAsync(Executor));
        }

        [Test]
        public async Task Select1SimpleLeftJoin()
        {
            // arrange
            // act
            var data = await Query<QueryClass>()
                .From(result => result.ThePerson)
                .LeftJoin<PersonClass>(result => result.ThePersonClasses)
                    .On((q, c) => q.ThePerson.Id == c.PersonId)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            AssertSelect1SimpleJoin(data);
        }

        [Test]
        public void UnmappedQuery_LeftJoinReturnsNull_ReturnsCorrectElements()
        {
            // arrange
            // act
            var data = Query<QueryContainer>()
                .From<Person>(x => x.ThePerson)
                .LeftJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.ThePerson.Id == ct.ClassId)
                .ToArray(Executor, logger: Logger);

            Assert.AreEqual(2, data.Length);

            Assert.AreEqual(Data.People.John, data[0].ThePerson);
            Assert.IsEmpty(data[0].TheClassTags);

            Assert.AreEqual(Data.People.Mary, data[1].ThePerson);
            Assert.IsEmpty(data[1].TheClassTags);
        }

        [Test]
        public void MappedQuery_LeftJoinReturnsNull_ReturnsCorrectElementsUTDUTDUTDUT()
        {
            // arrange
            // act
            var data = Query<QueryContainer>()
                .From<Person>(x => x.ThePerson)
                .LeftJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.ThePerson.Id == ct.ClassId)
                .Map(x => new
                {
                    thePerson = x.ThePerson.Name,
                    theClassTagIds = x.TheClassTags
                        .Select(y => y.ClassId)
                        .ToArray()
                })
                .ToArray(Executor, logger: Logger);

            Assert.AreEqual(2, data.Length);

            Assert.AreEqual(Data.People.John.Name, data[0].thePerson);
            Assert.IsEmpty(data[0].theClassTagIds);

            Assert.AreEqual(Data.People.Mary.Name, data[1].thePerson);
            Assert.IsEmpty(data[1].theClassTagIds);
        }

        class ClassesByTag
        {
            // warning CS0649: Field ... is never assigned to, and will always have its default value null
            #pragma warning disable 0649
            public Tag TheTag;
            public IEnumerable<Class> TheClasses;
            public IEnumerable<ClassTag> TheClassTags;
            #pragma warning restore 0649
        }

        [Test]
        public void MappedQuery_LeftJoinReturnsNullAndMappedToEnumerable_ReturnsCorrectElements()
        {
            // arrange
            // act
            var result = Query<ClassesByTag>()
                .From(x => x.TheTag)
                .LeftJoin(q => q.TheClassTags)
                    .On((q, ct) => q.TheTag.Id == ct.TagId)
                .LeftJoin(q => q.TheClasses)
                    .On((q, ct) => q.TheClassTags.One().ClassId == ct.Id)
                .Map(t => t.TheClasses
                    .Select(c => t.TheTag.Name))
                .ToArray(executor: Executor, logger: Logger);

            // assert
            CollectionAssert.AreEquivalent(new [] 
            { 
                Data.Tags.Sport.Name,
                Data.Tags.Sport.Name,
                Data.Tags.BallSport.Name
            }, result.SelectMany(xs => xs));
        }

        [Test]
        public async Task Select_JoinOnNonTable_ReturnsCorrectValues1()
        {
            // arrange
            // act
            var data = await Query<QueryClass>()
                .From(result => result.ThePerson)
                .InnerJoin<PersonClass>(result => result.ThePersonClasses)
                    .On((q, c) => c.ClassId == Data.Classes.Archery.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).ThePerson);

            foreach (var person in data)
            {
                Assert.AreEqual(1, person.ThePersonClasses.Count());
                Assert.AreEqual(Data.PersonClasses.JohnArchery, person.ThePersonClasses.First());
            }
        }

        [Test]
        public async Task Select_JoinOnNonTable_ReturnsCorrectValues2()
        {
            // arrange
            // act
            var data = await Query<QueryClass>()
                .From(result => result.ThePerson)
                .InnerJoin<PersonClass>(result => result.ThePersonClasses)
                    .On((q, c) => c.ClassId == Data.Classes.Tennis.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).ThePerson);

            foreach (var person in data)
            {
                Assert.AreEqual(2, person.ThePersonClasses.Count());
                Assert.AreEqual(Data.PersonClasses.JohnTennis, person.ThePersonClasses.First());
                Assert.AreEqual(Data.PersonClasses.MaryTennis, person.ThePersonClasses.ElementAt(1));
            }
        }

        [Test]
        public async Task Select_JoinTableAndNonTable_ReturnsCorrectValues()
        {
            // arrange
            // act
            var data = await Query<QueryClass>()
                .From(result => result.ThePerson)
                .InnerJoin<PersonClass>(result => result.ThePersonClasses)
                    .On((q, c) => q.ThePerson.Id == c.PersonId && c.ClassId == Data.Classes.Tennis.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).ThePerson);

            Assert.AreEqual(1, data.First().ThePersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis, data.First().ThePersonClasses.First());
            
            Assert.AreEqual(1, data.ElementAt(1).ThePersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.ElementAt(1).ThePersonClasses.First());
        }
    }
}