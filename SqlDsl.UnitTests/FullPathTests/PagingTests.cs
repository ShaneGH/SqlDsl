using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(SqlType.TSql)]
    [SqlTestAttribute(SqlType.MySql)]
    [SqlTestAttribute(SqlType.Sqlite)]
    public class PagingTests : FullPathTestBase
    {
        public PagingTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        [Test]
        public void Take_UnmappedQuery_PagesResults()
        {
            // arrange
            // act
            var data = Query<ClassTag>()
                .OrderBy(t => t.ClassId)
                .ThenBy(t => t.TagId)
                .Take(2)
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            {
                Data.ClassTags.TennisSport,
                Data.ClassTags.TennisBallSport,
            }, data);
        }

        [Test]
        public void SkipAndTake_UnmappedQuery_PagesResults()
        {
            // arrange
            // act
            var data = Query<ClassTag>()
                .OrderBy(t => t.ClassId)
                .ThenBy(t => t.TagId)
                .Skip(1).Take(2)
                .ToArray(Executor, logger: Logger);
                
            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                Data.ClassTags.TennisBallSport,
                Data.ClassTags.ArcherySport,
            }, data);
        }

        [Test]
        public void SkipAndTake_WithWhere_PagesResults()
        {
            // arrange
            // act
            var data = Query<ClassTag>()
                .Where(x => x.ClassId != 999)
                .OrderBy(t => t.ClassId)
                .ThenBy(t => t.TagId)
                .Skip(1).Take(2)
                .ToArray(Executor, logger: Logger);
                
            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                Data.ClassTags.TennisBallSport,
                Data.ClassTags.ArcherySport,
            }, data);
        }

        [Test]
        public void SkipAndTake_WithoutMap_PagesResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(SqlType, false)
                .OrderBy(x => x.ThePerson.Id)
                .Skip(1)
                .Take(1)
                .First(Executor, logger: Logger);
                
            // assert
            Assert.AreEqual(Data.People.Mary, data.ThePerson);
            Assert.AreEqual(Data.PeoplesData.MarysData, data.ThePersonsData);
            CollectionAssert.AreEquivalent(new [] { Data.PersonClasses.MaryTennis }, data.ThePersonClasses);
            CollectionAssert.AreEquivalent(new [] { Data.Classes.Tennis }, data.TheClasses);
            CollectionAssert.AreEquivalent(new [] { Data.ClassTags.TennisBallSport, Data.ClassTags.TennisSport }, data.TheClassTags);
            CollectionAssert.AreEquivalent(new [] { Data.Tags.Sport, Data.Tags.BallSport }, data.TheTags);
        }

        [Test]
        public void SkipAndTake_MappedQuery_PagesResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(SqlType, false)
                .Map(x => new
                {
                    person = x.ThePerson.Name,
                    classes = x.TheClasses.Count
                })
                .Skip(1)
                .Take(20)
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                new { person = Data.People.Mary.Name, classes = 1 },
            }, data);
        }

        [Test]
        public void SkipAndTake_UnmappedWithArgs_PagesResults()
        {
            // arrange
            // act
            var data = Query<(int skip, int take), ClassTag>()
                .OrderBy(x => x.ClassId)
                .ThenBy(x => x.TagId)
                .Skip(a => a.skip)
                .Take(a => a.take)
                .ToArray(Executor, (1, 2), logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                Data.ClassTags.TennisBallSport,
                Data.ClassTags.ArcherySport,
            }, data);
        }

        [Test]
        public void SkipAndTake_MappedWithArgs_PagesResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQueryWithArg<(int skip, int take)>(SqlType, false)
                .Map(x => new
                {
                    person = x.ThePerson.Name,
                    classes = x.TheClasses.Count
                })
                .Skip(a => a.skip)
                .Take(a => a.take)
                .ToArray(Executor, (1, 20), logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                new { person = Data.People.Mary.Name, classes = 1 },
            }, data);
        }
    }
}
