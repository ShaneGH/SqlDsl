using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using SqlDsl;
using SqlDsl.Mapper;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class OneTestsWithFunLogic : FullPathTestBase
    {
        [Test]
        public void GetOneTableAndMapProperty()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery()
                .Where(q => q.ThePersonClasses.One().ClassId == Data.Classes.Tennis.Id)
                .Map(p => p.ThePersonClasses.One().ClassId)
                .ToArray(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Length);
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data[0]);
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data[1]);
        }

        [Test]
        public void SelectFromTableAndMapResult()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery()
                .Where(q => q.ThePersonClasses.One().ClassId == Data.Classes.Tennis.Id)
                .Map(p => p.ThePersonClasses.Select(x => x.ClassId).One())
                .ToArray(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Length);
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data[0]);
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data[1]);
        }

        [Test]
        public void GetOneTableAndWhereOnProperty()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery()
                .Where(q => q.TheClasses.One().Id == Data.Classes.Tennis.Id)
                .Map(p => p.TheClasses)
                .ToArray(Executor, logger: Logger)
                .SelectMany(x => x);

            // assert
            CollectionAssert.AreEqual(data, new [] { Data.Classes.Tennis, Data.Classes.Tennis });
        }

        [Test]
        public void SelectFromTableAndWhereResult()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery()
                .Where(q => q.TheClasses.Select(x => x.Id).One() == Data.Classes.Tennis.Id)
                .Map(p => p.TheClasses)
                .ToArray(Executor, logger: Logger)
                .SelectMany(x => x);

            // assert
            CollectionAssert.AreEqual(data, new [] { Data.Classes.Tennis, Data.Classes.Tennis });
        }

        [Test]
        public void GetOneTableAndOrderByOnProperty()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery()
                .OrderBy(q => q.TheClasses.One().Name)
                .Map(p => p.TheClasses)
                .ToArray(Executor, logger: Logger)
                .SelectMany(x => x);

            // assert
            CollectionAssert.AreEqual(data, new [] { Data.Classes.Archery, Data.Classes.Tennis, Data.Classes.Tennis });
        }

        [Test]
        public void SelectFromTableAndOrderByResult()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery()
                .OrderBy(q => q.TheClasses.Select(x => x.Name).One())
                .Map(p => p.TheClasses)
                .ToArray(Executor, logger: Logger)
                .SelectMany(x => x);

            // assert
            CollectionAssert.AreEqual(data, new [] { Data.Classes.Archery, Data.Classes.Tennis, Data.Classes.Tennis });
        }

        [Test]
        public void WithCollectionTypeJoin_GetOneTableAndJoinOnProperty()
        {
            // arrange
            // act
            var data = Sql.Query.Sqlite<QueryContainer>()
                .From(x => x.ThePerson)
                .LeftJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pcs) => q.ThePerson.Id == pcs.PersonId)
                .LeftJoin<Class>(q => q.TheClasses)
                    .On((q, cl) => q.ThePersonClasses.One().ClassId == cl.Id)
                .First(Executor, logger: Logger);

            // assert
            Assert.AreEqual(data.ThePerson, Data.People.John);
            CollectionAssert.AreEqual(data.ThePersonClasses, new [] { Data.PersonClasses.JohnTennis, Data.PersonClasses.JohnArchery });
        }

        [Test]
        public void WithCollectionTypeJoin_SelectFromTableAndJoinOnResult()
        {
            // arrange
            // act
            var data = Sql.Query.Sqlite<QueryContainer>()
                .From(x => x.ThePerson)
                .LeftJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pcs) => q.ThePerson.Id == pcs.PersonId)
                .LeftJoin<Class>(q => q.TheClasses)
                    .On((q, cl) => q.ThePersonClasses.Select(x => x.ClassId).One() == cl.Id)
                .First(Executor, logger: Logger);

            // assert
            Assert.AreEqual(data.ThePerson, Data.People.John);
            CollectionAssert.AreEqual(data.ThePersonClasses, new [] { Data.PersonClasses.JohnTennis, Data.PersonClasses.JohnArchery });
        }

        [Test]
        public void OneOnConst_ThrowsException()
        {
            // arrange
            var somethingToOne = new List<int> { 1 };

            // act
            // assert
            Assert.Throws(typeof(SqlBuilderException), () => Sql.Query.Sqlite<Person>()
                .Map(x => somethingToOne.One())
                .First(Executor, logger: Logger));

        }

        [Test]
        public void OneOnArg_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(SqlBuilderException), () => Sql.Query.Sqlite<List<int>, Person>()
                .Map((x, a) => a.One())
                .First(Executor, new List<int> { 1 }, logger: Logger));

        }

        [Test]
        public void WithNonCollectionTypeJoin_GetOneTableAndOrderJoinOnProperty()
        {
            // arrange
            // act
            var data = Sql.Query.Sqlite<QueryContainer>()
                .From(x => x.ThePerson)
                .LeftJoin(q => q.ThePersonClasses)
                    .On((q, pcs) => q.ThePerson.Id == pcs.Select(pc => pc.PersonId).One())
                .First(Executor, logger: Logger);

            // assert
            Assert.AreEqual(data.ThePerson, Data.People.John);
            CollectionAssert.AreEqual(data.ThePersonClasses, new [] { Data.PersonClasses.JohnTennis, Data.PersonClasses.JohnArchery });
        }
    }
}
