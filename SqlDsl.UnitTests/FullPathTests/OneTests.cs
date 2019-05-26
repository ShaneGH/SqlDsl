using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using SqlDsl;
using SqlDsl.Mapper;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(SqlType.Sqlite)]
    public class OneTestsWithFunLogic : FullPathTestBase
    {
        public OneTestsWithFunLogic(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        [Test]
        public void GetOneTableAndMapProperty()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(SqlType)
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
                .FullyJoinedQuery(SqlType)
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
                .FullyJoinedQuery(SqlType)
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
                .FullyJoinedQuery(SqlType)
                .Where(q => q.TheClasses.Select(x => x.Id).One() == Data.Classes.Tennis.Id)
                .Map(p => p.TheClasses)
                .ToArray(Executor, logger: Logger)
                .SelectMany(x => x)
                .ToArray();

            // assert
            CollectionAssert.AreEqual(new [] { Data.Classes.Tennis, Data.Classes.Tennis }, data);
        }

        [Test]
        public void GetOneTableAndOrderByOnProperty()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(SqlType)
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
                .FullyJoinedQuery(SqlType)
                .OrderBy(q => q.TheClasses.Select(x => x.Name).One())
                .Map(p => p.TheClasses)
                .ToArray(Executor, logger: Logger)
                .SelectMany(x => x);

            // assert
            CollectionAssert.AreEqual(data, new [] { Data.Classes.Archery, Data.Classes.Tennis, Data.Classes.Tennis });
        }

        [Test]
        public void WithNonCollectionTypeJoin_GetOneTableAndOrderJoinOnProperty()
        {
            // arrange
            // act
            var data = Query<QueryContainer>()
                .From(x => x.ThePerson)
                .LeftJoin(q => q.ThePersonClasses)
                    .On((q, pcs) => q.ThePerson.Id == pcs.One().PersonId)
                .First(Executor, logger: Logger);

            // assert
            Assert.AreEqual(data.ThePerson, Data.People.John);
            CollectionAssert.AreEqual(data.ThePersonClasses, new [] { Data.PersonClasses.JohnTennis, Data.PersonClasses.JohnArchery });
        }

        [Test]
        public void WithNonCollectionTypeJoin_SelectFromTableAndJoinOnResult()
        {
            // arrange
            // act
            var data = Query<QueryContainer>()
                .From(x => x.ThePerson)
                .LeftJoin(q => q.ThePersonClasses)
                    .On((q, pcs) => q.ThePerson.Id == pcs.Select(pc => pc.PersonId).One())
                .First(Executor, logger: Logger);

            // assert
            Assert.AreEqual(data.ThePerson, Data.People.John);
            CollectionAssert.AreEqual(data.ThePersonClasses, new [] { Data.PersonClasses.JohnTennis, Data.PersonClasses.JohnArchery });
        }

        [Test]
        public void WithCollectionTypeJoin_GetOneTableAndJoinOnProperty()
        {
            // arrange
            // act
            var data = Query<QueryContainer>()
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
            var data = Query<QueryContainer>()
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

        public class LoadsOfData
        {
            public PersonsData Data1;
            public List<PersonsData> Data2;
            public List<PersonsData> Data3;
        }

        [Test]
        public void WithCollectionTypeJoinAndCollectionTypeData_GetOneTableAndJoinOnProperty()
        {
            // arrange
            // act
            var data = Query<LoadsOfData>()
                .From(x => x.Data1)
                .LeftJoin(q => q.Data2)
                    .On((q, d) => q.Data1.Data == d.One().Data)
                .LeftJoin(q => q.Data3)
                    .On((q, d) => q.Data2.One().Data == d.One().Data)
                .First(Executor, logger: Logger);

            // assert
            Assert.AreEqual(data.Data1, Data.PeoplesData.JohnsData);
            CollectionAssert.AreEqual(data.Data2, new [] { Data.PeoplesData.JohnsData });
            CollectionAssert.AreEqual(data.Data3, new [] { Data.PeoplesData.JohnsData });
        }

        [Test]
        public void WithCollectionTypeJoinAndCollectionTypeData_SelectFromTableAndJoinOnResult()
        {
            // arrange
            // act
            var data = Query<LoadsOfData>()
                .From(x => x.Data1)
                .LeftJoin(q => q.Data2)
                    .On((q, d) => q.Data1.Data == d.Select(x => x.Data).One())
                .LeftJoin(q => q.Data3)
                    .On((q, d) => q.Data2.Select(x => x.Data).One() == d.Select(x => x.Data).One())
                .First(Executor, logger: Logger);

            // assert
            Assert.AreEqual(data.Data1, Data.PeoplesData.JohnsData);
            CollectionAssert.AreEqual(data.Data2, new [] { Data.PeoplesData.JohnsData });
            CollectionAssert.AreEqual(data.Data3, new [] { Data.PeoplesData.JohnsData });
        }

        [Test]
        public void OneOnConst_ThrowsException()
        {
            // arrange
            var somethingToOne = new List<int> { 1 };

            // act
            // assert
            Assert.Throws(typeof(SqlBuilderException), () => Query<Person>()
                .Map(x => somethingToOne.One())
                .First(Executor, logger: Logger));

        }

        [Test]
        public void OneOnArg_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(SqlBuilderException), () => Query<List<int>, Person>()
                .Map((x, a) => a.One())
                .First(Executor, new List<int> { 1 }, logger: Logger));
        }
    }
}
