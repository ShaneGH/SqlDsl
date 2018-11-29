using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;
using NUnit.Framework;
using SqlDsl.Utils;
using SqlDsl.UnitTests.FullPathTests.Environment;
using SqlDsl.Sqlite;
using NUnit.Framework.Interfaces;

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class WhereConditionTests : FullPathTestBase
    {
        class QueryClass
        {
            public Person Person { get; set; }
            public IEnumerable<PersonClass> PersonClasses { get; set; }
        }

        [Test]
        public async Task Select1SimpleObject()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)                
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereEquality()
        {
            // arrange
            // actt
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where(result => result.Person.Id == Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereNonEquality()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where(result => result.Person.Id != Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereGT()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where(result => result.Person.Id > Data.People.John.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereGTEq()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where(result => result.Person.Id >= Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereLT()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where(result => result.Person.Id < Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereLTEq()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where(result => result.Person.Id <= Data.People.John.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn1()
        {
            // arrange
            var inVals = new [] { Data.People.John.Id, 1000 };
            
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where(result => result.Person.Id.In(inVals))
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereEmptyIn()
        {
            // arrange
            var inVals = new long[0];
            
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where(result => result.Person.Id.In(inVals))
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
                .From(nameof(Person), result => result.Person)
                .Where(result => result.Person.Id.In(new [] { Data.People.John.Id }))
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn2_2()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where(result => result.Person.Id.In(new [] { Data.People.John.Id, 1000 }))
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn3()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<int, QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where((result, args) => result.Person.Id.In(new [] { Data.People.John.Id, args }))
                .ToArrayAsync(Executor, 1000, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn4()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<long[], QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where((result, args) => result.Person.Id.In(args))
                .ToArrayAsync(Executor, new long[] { Data.People.John.Id, 1000 }, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereEmptyIn2()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<long[], QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where((result, args) => result.Person.Id.In(args))
                .ToArrayAsync(Executor, new long[0], logger: Logger);

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
                .From(nameof(Person), result => result.Person)
                .Where(result => inVals.Contains(result.Person.Id))
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn6()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where(result => new [] { Data.People.John.Id }.Contains(result.Person.Id))
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereIn7()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .InnerJoin(x => x.PersonClasses).On((q, pc) => pc.PersonId.In(new [] { Data.People.Mary.Id }))
                .Where(result => result.Person.Id == Data.People.Mary.Id)
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereInList()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where(result => result.Person.Id.In(new List<long> { Data.People.John.Id }))
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereInList_2()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where(result => result.Person.Id.In(new List<long> { Data.People.John.Id, 1000 }))
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
        }

        [Test]
        public void Select1SimpleObject_WithJoinOnIn4()
        {
            // arrange
            // act
            var data = Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .InnerJoin(x => x.PersonClasses).On((q, pc) => pc.PersonId.In(new [] { Data.People.John.Id }))
                .ToArray(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data[0].Person);
            Assert.AreEqual(Data.People.Mary, data[1].Person);
            
            CollectionAssert.AreEqual(new [] 
            { 
                Data.PersonClasses.JohnTennis,
                Data.PersonClasses.JohnArchery
            }, data[0].PersonClasses);

            CollectionAssert.AreEqual(new [] 
            { 
                Data.PersonClasses.JohnTennis,
                Data.PersonClasses.JohnArchery
            }, data[1].PersonClasses);
        }

        [Test]
        public async Task SelectJoinedObject_WithWhereOnJoin()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .InnerJoin<PersonClass>(q => q.PersonClasses)
                    .On((q, c) => q.Person.Id == c.PersonId)
                .Where(result => result.PersonClasses.One().ClassId == Data.Classes.Archery.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
            Assert.AreEqual(1, data.First().PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.JohnArchery, data.First().PersonClasses.First());
        }
    }
}
