using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class WhereConditionTests : FullPathTestBase
    {
        class QueryClass
        {
            public Person ThePerson { get; set; }
            public IEnumerable<PersonClass> ThePersonClasses { get; set; }
        }

        [Test]
        public async Task Select1Entity()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<Person>()
                .Where(x => x.Id == Data.People.Mary.Id)
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data[0]);
        }

        [Test]
        public async Task Select1SimpleObject()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)                
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereEquality()
        {
            // arrange
            // actt
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id == Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereNonEquality()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id != Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereGT()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id > Data.People.John.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereGTEq()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id >= Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereLT()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id < Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereLTEq()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id <= Data.People.John.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn1()
        {
            // arrange
            var inVals = new [] { Data.People.John.Id, 1000 };
            
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id.In(inVals))
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereEmptyIn()
        {
            // arrange
            var inVals = new long[0];
            
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id.In(inVals))
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(0, data.Count());
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn2()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id.In(new [] { Data.People.John.Id }))
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn2_2()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id.In(new [] { Data.People.John.Id, 1000 }))
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn2_2WithMultipleParts()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id.In(new [] { Data.People.John.Id + 1, 1000 + 1 }))
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn3()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<int, QueryClass>()
                .From(result => result.ThePerson)
                .Where((result, args) => result.ThePerson.Id.In(new [] { Data.People.John.Id, args }))
                .ToArrayAsync(Executor, 1000, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn4()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<long[], QueryClass>()
                .From(result => result.ThePerson)
                .Where((result, args) => result.ThePerson.Id.In(args))
                .ToArrayAsync(Executor, new long[] { Data.People.John.Id, 1000 }, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereEmptyIn2()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<long[], QueryClass>()
                .From(result => result.ThePerson)
                .Where((result, args) => result.ThePerson.Id.In(args))
                .ToArrayAsync(Executor, new long[0], logger: Logger);

            // assert
            Assert.AreEqual(0, data.Count());
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereEmptyIn3()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id.In(new long[0]))
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(0, data.Count());
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn5()
        {
            // arrange
            var inVals = new [] { Data.People.John.Id };

            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => inVals.Contains(result.ThePerson.Id))
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn6()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => new [] { Data.People.John.Id }.Contains(result.ThePerson.Id))
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn7()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .InnerJoin(x => x.ThePersonClasses).On((q, pc) => pc.PersonId.In(new [] { Data.People.Mary.Id }))
                .Where(result => result.ThePerson.Id == Data.People.Mary.Id)
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereInList()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id.In(new List<long> { Data.People.John.Id }))
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereInList_2()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id.In(new List<long> { Data.People.John.Id, 1000 }))
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
        }

        [Test]
        public void Select1SimpleObject_WithJoinOnIn4()
        {
            // arrange
            // act
            var data = Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .InnerJoin(x => x.ThePersonClasses).On((q, pc) => pc.PersonId.In(new [] { Data.People.John.Id }))
                .ToArray(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data[0].ThePerson);
            Assert.AreEqual(Data.People.Mary, data[1].ThePerson);
            
            CollectionAssert.AreEqual(new [] 
            { 
                Data.PersonClasses.JohnTennis,
                Data.PersonClasses.JohnArchery
            }, data[0].ThePersonClasses);

            CollectionAssert.AreEqual(new [] 
            { 
                Data.PersonClasses.JohnTennis,
                Data.PersonClasses.JohnArchery
            }, data[1].ThePersonClasses);
        }

        [Test]
        public async Task SelectJoinedObject_WithWhereOnJoin()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(result => result.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, c) => q.ThePerson.Id == c.PersonId)
                .Where(result => result.ThePersonClasses.One().ClassId == Data.Classes.Archery.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
            Assert.AreEqual(1, data.First().ThePersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.JohnArchery, data.First().ThePersonClasses.First());
        }
    }
}
