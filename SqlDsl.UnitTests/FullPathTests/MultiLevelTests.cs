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
    public class ObjectShapeTests
    {
        class QueryClass1
        {
            public Person Person { get; set; }
            public IEnumerable<PersonClass> PersonClasses { get; set; }
        }
        
        class QueryClass2
        {
            public QueryClass1 Inner { get; set; }
        }
        
        class QueryClass3
        {
            public List<QueryClass1> Inner { get; set; }
        }

        class ZeroLevels : Person
        {
            public PersonClass[] PersonClasses { get; set; }
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
        public async Task SelectWith0Levels()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<ZeroLevels>()
                .From(result => result)
                .InnerJoin<PersonClass>(p => p.PersonClasses)
                .On((person, classes) => person.Id == classes.PersonId)
                .ExecuteAsync(new SqliteExecutor(Connection));

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1));
            
            Assert.AreEqual(2, data.First().PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.JohnArchery, data.First().PersonClasses.First());
            Assert.AreEqual(Data.PersonClasses.JohnArchery, data.First().PersonClasses.ElementAt(1));

            Assert.AreEqual(1, data.ElementAt(1).PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.ElementAt(1).PersonClasses.First());
        }

        [Test]
        public async Task SelectWith2Levels()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass2>()
                .From(result => result.Inner.Person)
                .ExecuteAsync(new SqliteExecutor(Connection));

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Inner.Person);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).Inner.Person);
        }

        [Test]
        public async Task WhereWith2Levels()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass2>()
                .From(result => result.Inner.Person)
                .Where(result => result.Inner.Person.Id == Data.People.Mary.Id)
                .ExecuteAsync(new SqliteExecutor(Connection));

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(0).Inner.Person);
        }

        [Test]
        public async Task JoinWith2Levels()
        {
            // arrange
            var ex = new TestExecutor(new SqliteExecutor(Connection));

            // act
            var data = await Sql.Query.Sqlite<QueryClass2>()
                .From(result => result.Inner.Person)
                .LeftJoin<PersonClass>(result => result.Inner.PersonClasses)
                    .On((r, pc) => r.Inner.Person.Id == pc.PersonId)
                .Where(result => result.Inner.Person.Id == Data.People.Mary.Id)
                .ExecuteAsync(ex);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(0).Inner.Person);
            Assert.AreEqual(1, data.First().Inner.PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.First().Inner.PersonClasses.First());
        }

        [Test]
        public async Task JoinWith2LevelsAndList()
        {
            // arrange
            var ex = new TestExecutor(new SqliteExecutor(Connection));

            // act
            var data = await Sql.Query.Sqlite<QueryClass3>()
                .From(result => result.Inner.One().Person)
                .LeftJoin<PersonClass>(result => result.Inner.One().PersonClasses)
                    .On((r, pc) => r.Inner.One().Person.Id == pc.PersonId)
                .Where(result => result.Inner.One().Person.Id == Data.People.Mary.Id)
                .ExecuteAsync(ex);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(1, data.ElementAt(0).Inner.Count());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(0).Inner.First().Person);
            Assert.AreEqual(1, data.First().Inner.First().PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.First().Inner.First().PersonClasses.First());
        }
    }
}
