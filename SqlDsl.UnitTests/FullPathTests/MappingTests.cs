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
    public class MappingTests : FullPathTestBase
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
                .ExecuteAsync(Executor);

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
                .ExecuteAsync(Executor);

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
                .ExecuteAsync(Executor);

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
                .ExecuteAsync(Executor);

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
                .ExecuteAsync(Executor);

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
                .ExecuteAsync(Executor);

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
                .ExecuteAsync(Executor);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
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
                .ExecuteAsync(Executor);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
            Assert.AreEqual(1, data.First().PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.JohnArchery, data.First().PersonClasses.First());
        }
    }
}
