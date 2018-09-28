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
    public class ObjectShapeTests : FullPathTestBase
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

        [Test]
        public async Task SelectWith0Levels()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<Person>()
                .From(result => result)
                .ExecuteAsync(Executor);
                
            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1));
        }

        [Test]
        public async Task SelectWith1Levels()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass1>()
                .From(result => result.Person)
                .ExecuteAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).Person);
        }

        [Test]
        public async Task SelectWith2Levels()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass2>()
                .From(result => result.Inner.Person)
                .ExecuteAsync(Executor);

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
                .ExecuteAsync(Executor);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(0).Inner.Person);
        }

        [Test]
        public async Task JoinWith2Levels()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass2>()
                .From(result => result.Inner.Person)
                .LeftJoin<PersonClass>(result => result.Inner.PersonClasses)
                    .On((r, pc) => r.Inner.Person.Id == pc.PersonId)
                .Where(result => result.Inner.Person.Id == Data.People.Mary.Id)
                .ExecuteAsync(Executor);

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
            // act
            var data = await Sql.Query.Sqlite<QueryClass3>()
                .From(result => result.Inner.SingleOrDefault().Person)
                .LeftJoin<PersonClass>(result => result.Inner.One().PersonClasses)
                    .On((r, pc) => r.Inner.FirstOrDefault().Person.Id == pc.PersonId)
                .Where(result => result.Inner.One().Person.Id == Data.People.Mary.Id)
                .ExecuteAsync(Executor);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(1, data.ElementAt(0).Inner.Count());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(0).Inner.First().Person);
            Assert.AreEqual(1, data.First().Inner.First().PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.First().Inner.First().PersonClasses.First());
        }

        class QueryClass5
        {
            public Person Person { get; set; }
            public PersonClass PersonClass { get; set; }
        }

        [Test]
        public async Task JoinTableIsNotList_1ResultReturned_MapsCorrectly()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass5>()
                .From(result => result.Person)
                .LeftJoin<PersonClass>(result => result.PersonClass)
                    .On((r, pc) => r.Person.Id == pc.PersonId)
                .Where(result => result.Person.Id == Data.People.Mary.Id)
                .ExecuteAsync(Executor);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.First().Person);
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.First().PersonClass);
        }

        [Test]
        public void JoinTableIsNotList_MoreThan1ResultReturned_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(InvalidOperationException), () => Sql.Query.Sqlite<QueryClass5>()
                .From(result => result.Person)
                .LeftJoin<PersonClass>(result => result.PersonClass)
                    .On((r, pc) => r.Person.Id == pc.PersonId)
                .Where(result => result.Person.Id == Data.People.John.Id)
                .ExecuteAsync(Executor));
        }

        class WhereErrorQueryClass
        {
            // warning CS0649: Field 'ObjectShapeTests.WhereErrorQueryClass.Person2' is never assigned to, and will always have its default value null
            #pragma warning disable 0649
            public Person Person1;
            public Person Person2;
            #pragma warning restore 0649
        }

        [Test]
        public void Select_JoinComparrisonComparesComplexObjects_ThrowsError()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(NotImplementedException), () =>
                Sql.Query.Sqlite<WhereErrorQueryClass>()
                    .From(result => result.Person1)
                    .InnerJoin(result => result.Person2)
                        .On((q, p) => q.Person1 == p)
                    .Compile());
        }

        [Test]
        public void Select_WhereComparrisonComparesComplexObjects_ThrowsError()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(SqliteException), () =>
                Sql.Query.Sqlite<WhereErrorQueryClass>()
                    .From(result => result.Person1)
                    .InnerJoin(result => result.Person2)
                        .On((q, p) => q.Person1.Id == p.Id)
                    .Where(q => q.Person1 == q.Person2)
                    .ExecuteAsync(Executor));
        }
    }
}
