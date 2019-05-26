using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.Mapper;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(TestFlavour.Sqlite)]
    public class PagingAndRowNumberTests : FullPathTestBase
    {
        public PagingAndRowNumberTests(TestFlavour testFlavour)
            : base(testFlavour)
        {
        }
        
        [Test]
        public void Take_UnmappedQuery_PagesResults()
        {
            // arrange
            // act
            var data = Query<ClassTag>()
                .Take(2)
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            {
                Data.ClassTags.ArcherySport,
                Data.ClassTags.TennisSport,
            }, data);
        }

        [Test]
        public void SkipAndTake_UnmappedQuery_PagesResults()
        {
            // arrange
            // act
            var data = Query<ClassTag>()
                .Skip(1).Take(2)
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                Data.ClassTags.TennisSport,
                Data.ClassTags.TennisBallSport,
            }, data);
        }

        [Test]
        public void SkipAndTake_WithWhere_PagesResults()
        {
            // arrange
            // act
            var data = Query<ClassTag>()
                .Where(x => x.ClassId != 999)
                .Skip(1).Take(2)
                .ToArray(Executor, logger: Logger);
                
            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                Data.ClassTags.TennisSport,
                Data.ClassTags.TennisBallSport,
            }, data);
        }

        [Test]
        public void SkipAndTake_WithoutMap_PagesResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(TestFlavour, false)
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
                .FullyJoinedQuery(TestFlavour, false)
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
                .Skip(a => a.skip)
                .Take(a => a.take)
                .ToArray(Executor, (1, 2), logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                Data.ClassTags.TennisSport,
                Data.ClassTags.TennisBallSport,
            }, data);
        }

        [Test]
        public void SkipAndTake_MappedWithArgs_PagesResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQueryWithArg<(int skip, int take)>(TestFlavour, false)
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

        [Test]
        public void Where_WithRowNumber_PagesResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(TestFlavour, false)
                .Where(x => x.RowNumber() == 2)
                .Map(x => new
                {
                    person = x.ThePerson.Name,
                    classes = x.TheClasses.Count
                })
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                new { person = Data.People.Mary.Name, classes = 1 },
            }, data);
        }

        [Test]
        public void Map_WithRowNumber_PagesResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(x => new
                {
                    person = x.ThePerson,
                    rowNumber = x.RowNumber()
                })
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                new { person = Data.People.John, rowNumber = 1 },
                new { person = Data.People.Mary, rowNumber = 2 },
            }, data);
        }

        [Test]
        public void Map_WithNullableRowNumber_PagesResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(x => new
                {
                    person = x.ThePerson,
                    rowNumber = x.NullableRowNumber()
                })
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                new { person = Data.People.John, rowNumber = (int?)1 },
                new { person = Data.People.Mary, rowNumber = (int?)2 },
            }, data);
        }

        [Test]
        public void Map_WithRowNumberInInnerEntity_MapsResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(x => new
                {
                    person = x.ThePerson,
                    classes = x.TheClasses
                        .Select(c => new
                        {
                            name = c.Name,
                            rowNumber1 = x.RowNumber(),
                            rowNumber2 = c.RowNumber()
                        })
                        .ToArray()
                })
                .ToArray(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Length);
            Assert.AreEqual(Data.People.John, data[0].person);
            Assert.AreEqual(Data.People.Mary, data[1].person);

            CollectionAssert.AreEqual(new[] 
            { 
                new { name = Data.Classes.Tennis.Name, rowNumber1 = 1, rowNumber2 = 1 },
                new { name = Data.Classes.Archery.Name, rowNumber1 = 1, rowNumber2 = 2 }
            }, data[0].classes);

            CollectionAssert.AreEqual(new[] 
            { 
                new { name = Data.Classes.Tennis.Name, rowNumber1 = 2, rowNumber2 = 1 }
            }, data[1].classes);
        }

        [Test]
        public void Map_OnlyRowNumber_MapsResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(x => x.RowNumber())
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[]{1,2}, data);
        }

        [Test]
        public void Map_RowNumberOnProperty_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.Throws(
                typeof(SqlBuilderException), 
                () => TestUtils
                    .FullyJoinedQuery(TestFlavour)
                    .Map(x => x.ThePerson.Name.RowNumber())
                    .ToArray(Executor, logger: Logger));
        }

        [Test]
        public void Map_OnlySingleInnerRowNumber_MapsResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(x => x.ThePersonsData.RowNumber())
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[]{1,2}, data);
        }

        [Test]
        public void Map_OnlyMultiInnerRowNumber_MapsResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(x => x.TheClasses.Select(c => c.RowNumber()))
                .First(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[]{1,2}, data);
        }

        [Test]
        public void Map_WithRowNumberOnLeftJoin_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.Throws(
                typeof(SqlBuilderException), 
                () => TestUtils
                    .FullyLeftJoinedQuery(TestFlavour)
                    .Map(x => new
                    {
                        person = x.ThePerson,
                        rowNumbers = x.TheClasses.Select(c => c.RowNumber())
                    })
                    .ToArray(Executor, logger: Logger));
        }
    }
}
