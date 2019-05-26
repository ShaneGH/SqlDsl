using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using SqlDsl.Mapper;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(TestFlavour.Sqlite)]
    public class SqlCaseTests : FullPathTestBase
    {
        public SqlCaseTests(TestFlavour testFlavour)
            : base(testFlavour)
        {
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
        public void SqlCase_WithPlainExpressions_ReturnsCorrectResult()
        {
            // arrange
            // act
            var result = Sql.Query.Sqlite<ClassesByTag>()
                .From(x => x.TheTag)
                .LeftJoin(q => q.TheClassTags)
                    .On((q, ct) => q.TheTag.Id == ct.TagId)
                .LeftJoin(q => q.TheClasses)
                    .On((q, ct) => q.TheClassTags.One().ClassId == ct.Id)
                .Map(t => t.TheClasses
                    .Select(c => new
                    {
                        n = t.TheTag.Name,
                        r = t.TheTag.Name == Data.Tags.Sport.Name
                            ? c.Name == Data.Classes.Tennis.Name
                                ? 1
                                : 10
                            : t.TheTag.Name == Data.Tags.BallSport.Name
                                ? 100
                                : 1000  // it is hard to test this case. Using as smoke test only
                    }))
                .ToArray(executor: Executor, logger: Logger)
                .SelectMany(xs => xs);

            // assert
            CollectionAssert.AreEquivalent(new [] 
            { 
                new { n = Data.Tags.Sport.Name, r = 1 }, 
                new { n = Data.Tags.Sport.Name, r = 10 }, 
                new { n = Data.Tags.BallSport.Name, r = 100 }
            }, result);
        }

        class PersonsDataTwice
        {
            // warning CS0649: Field ... is never assigned to, and will always have its default value null
            #pragma warning disable 0649
            public Person ThePerson;
            public PersonsData ThePersonsData1;
            public PersonsData ThePersonsData2;
            #pragma warning restore 0649
        }

        [Test]
        public void SqlCase_WithCaseReturnsTable_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(SqlBuilderException), () => Sql.Query.Sqlite<PersonsDataTwice>()
                .From(x => x.ThePerson)
                .LeftJoin(q => q.ThePersonsData1)
                    .On((q, ct) => q.ThePerson.Id == ct.PersonId)
                .LeftJoin(q => q.ThePersonsData2)
                    .On((q, ct) => q.ThePerson.Id == ct.PersonId)
                .Map(t => (t.ThePerson.Name == "a"
                    ? t.ThePersonsData1
                    : t.ThePersonsData2).Data)
                .ToArray(executor: Executor, logger: Logger));

        }

        [Test]
        public void SqlCase_WithCaseDsl_ReturnsCorrectResult()
        {
            // arrange
            // act
            var result = Sql.Query
                .Sqlite<Tag>()
                .Map(t => new
                {
                    n = t.Name,
                    cs = Sql.Case
                        .When(t.Name == Data.Tags.BallSport.Name)
                        .Then(1)
                        .When(t.Name == Data.Tags.Sport.Name)
                        .Then(10)
                        .Else(100)
                })
                .ToArray(executor: Executor, logger: Logger);

            // assert
            CollectionAssert.AreEquivalent(new [] 
            { 
                new { n = Data.Tags.BallSport.Name, cs = 1 }, 
                new { n = Data.Tags.Sport.Name, cs = 10 }, 
                new { n = Data.Tags.UnusedTag.Name, cs = 100 }
            }, result);
        }

        [Test]
        public void SqlCase_WithCaseDslReturnsTable_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(SqlBuilderException), () => Sql.Query.Sqlite<PersonsDataTwice>()
                .From(x => x.ThePerson)
                .LeftJoin(q => q.ThePersonsData1)
                    .On((q, ct) => q.ThePerson.Id == ct.PersonId)
                .LeftJoin(q => q.ThePersonsData2)
                    .On((q, ct) => q.ThePerson.Id == ct.PersonId)
                .Map(t => Sql.Case
                    .When(t.ThePerson.Name == "a")
                    .Then(t.ThePersonsData1)
                    .Else(t.ThePersonsData2).Data)
                .ToArray(executor: Executor, logger: Logger));
        }
        
        [Test]
        public void SqlSimpleCase_ReturnsCorrectResult()
        {
            // arrange
            // act
            var result = Sql.Query
                .Sqlite<Tag>()
                .Map(t => new
                {
                    n = t.Name,
                    cs = Sql.Case
                        .Simple(t.Name)
                        .When(Data.Tags.BallSport.Name)
                        .Then(1)
                        .When(Data.Tags.Sport.Name)
                        .Then(10)
                        .Else(100)
                })
                .ToArray(executor: Executor, logger: Logger);

            // assert
            CollectionAssert.AreEquivalent(new [] 
            { 
                new { n = Data.Tags.BallSport.Name, cs = 1 }, 
                new { n = Data.Tags.Sport.Name, cs = 10 }, 
                new { n = Data.Tags.UnusedTag.Name, cs = 100 }
            }, result);
        }

        [Test]
        public void SimpleCase_WithCaseReturnsTable_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(SqlBuilderException), () => Sql.Query.Sqlite<PersonsDataTwice>()
                .From(x => x.ThePerson)
                .LeftJoin(q => q.ThePersonsData1)
                    .On((q, ct) => q.ThePerson.Id == ct.PersonId)
                .LeftJoin(q => q.ThePersonsData2)
                    .On((q, ct) => q.ThePerson.Id == ct.PersonId)
                .Map(t => Sql.Case
                    .Simple(t.ThePerson.Name)
                    .When("a")
                    .Then(t.ThePersonsData1)
                    .Else(t.ThePersonsData2).Data)
                .ToArray(executor: Executor, logger: Logger));
        }
    }
}