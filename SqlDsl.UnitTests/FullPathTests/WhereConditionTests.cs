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

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class WhereConditionTests
    {
        class QueryClass
        {
            public Person Person { get; set; }
        }

        SqliteConnection Connection;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            InitData.EnsureInit();
            Connection = InitData.CreateConnection();
            Connection.Open();
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            Connection.Dispose();
        }

        [Test]
        public async Task Select1SimpleObject()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)                
                .ExecuteAsync(new SqliteExecutor(Connection));

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).Person);
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereEquality()
        {
            // arrange
            // act
            var ex = new TestExecutor(new SqliteExecutor(Connection));
            try
            {
                var data = await Sql.Query.Sqlite<QueryClass>()
                    .From(nameof(Person), result => result.Person)
                    .Where(result => result.Person.Id == Data.People.Mary.Id)
                    .ExecuteAsync(ex);

                // assert
                Assert.AreEqual(1, data.Count());
                Assert.AreEqual(Data.People.Mary, data.First().Person);
            }
            catch 
            {
                ex.PrintSqlStatements();
                throw;
            }
        }

        [Test]
        public async Task Select1SimpleObject_WithWhereNonEquality()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .Where(result => result.Person.Id != Data.People.Mary.Id)
                .ExecuteAsync(new SqliteExecutor(Connection));

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
                .ExecuteAsync(new SqliteExecutor(Connection));

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
                .ExecuteAsync(new SqliteExecutor(Connection));

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
                .ExecuteAsync(new SqliteExecutor(Connection));

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
                .ExecuteAsync(new SqliteExecutor(Connection));

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
        }
    }
}
