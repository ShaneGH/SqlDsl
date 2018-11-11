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
    public class JoinConditionTests : FullPathTestBase
    {
        class QueryClass
        {
            public Person Person { get; set; }
            public IEnumerable<PersonClass> PersonClasses { get; set; }
            public IEnumerable<ClassTag> ClassTags { get; set; }
        }

        void AssertSelect1SimpleJoin(IEnumerable<QueryClass> data)
        {
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).Person);

            Assert.AreEqual(2, data.First().PersonClasses.Count());
            Assert.Contains(Data.PersonClasses.JohnTennis, data.First().PersonClasses.ToList());
            Assert.Contains(Data.PersonClasses.JohnArchery, data.First().PersonClasses.ToList());

            Assert.AreEqual(1, data.ElementAt(1).PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.ElementAt(1).PersonClasses.ElementAt(0));
        }

        [Test]
        public async Task Select1SimpleInnerJoin()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .InnerJoin<PersonClass>(nameof(PersonClass), result => result.PersonClasses)
                    .On((q, c) => q.Person.Id == c.PersonId)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            AssertSelect1SimpleJoin(data);
        }

        [Test]
        public void With1Join_MissingConnectingJoin_ThrowsError()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(InvalidOperationException), () =>
                Sql.Query.Sqlite<QueryClass>()
                    .From<Person>(x => x.Person)
                    .InnerJoin<ClassTag>(q => q.ClassTags)
                        .On((q, ct) => q.PersonClasses.One().ClassId == ct.ClassId)
                    .ToIEnumerableAsync(Executor));
        }

        [Test]
        public async Task Select1SimpleLeftJoin()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .LeftJoin<PersonClass>(nameof(PersonClass), result => result.PersonClasses)
                    .On((q, c) => q.Person.Id == c.PersonId)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            AssertSelect1SimpleJoin(data);
        }

        [Test]
        public async Task Select2Joins_DoesNotDuplicateRecords()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .InnerJoin<PersonClass>(nameof(PersonClass), result => result.PersonClasses)
                    .On((q, c) => q.Person.Id == c.PersonId)
                .InnerJoin<ClassTag>(nameof(ClassTag), result => result.ClassTags)
                    .On((q, c) => q.PersonClasses.First().ClassId == c.ClassId)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            AssertSelect1SimpleJoin(data);
            
            Assert.AreEqual(3, data.First().ClassTags.Count());
            Assert.AreEqual(Data.ClassTags.TennisSport, data.First().ClassTags.ElementAt(0));
            Assert.AreEqual(Data.ClassTags.TennisBallSport, data.First().ClassTags.ElementAt(1));
            Assert.AreEqual(Data.ClassTags.ArcherySport, data.First().ClassTags.ElementAt(2));

            Assert.AreEqual(2, data.ElementAt(1).ClassTags.Count());
            Assert.AreEqual(Data.ClassTags.TennisSport, data.First().ClassTags.ElementAt(0));
            Assert.AreEqual(Data.ClassTags.TennisBallSport, data.First().ClassTags.ElementAt(1));
        }

        [Test]
        public async Task Select2Joins_backwards_DoesNotDuplicateRecords()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .InnerJoin<ClassTag>(nameof(ClassTag), result => result.ClassTags)
                    .On((q, c) => q.PersonClasses.Single().ClassId == c.ClassId)
                .InnerJoin<PersonClass>(nameof(PersonClass), result => result.PersonClasses)
                    .On((q, c) => q.Person.Id == c.PersonId)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            AssertSelect1SimpleJoin(data);
            
            Assert.AreEqual(3, data.First().ClassTags.Count());
            Assert.AreEqual(Data.ClassTags.TennisSport, data.First().ClassTags.ElementAt(0));
            Assert.AreEqual(Data.ClassTags.TennisBallSport, data.First().ClassTags.ElementAt(1));
            Assert.AreEqual(Data.ClassTags.ArcherySport, data.First().ClassTags.ElementAt(2));

            Assert.AreEqual(2, data.ElementAt(1).ClassTags.Count());
            Assert.AreEqual(Data.ClassTags.TennisSport, data.First().ClassTags.ElementAt(0));
            Assert.AreEqual(Data.ClassTags.TennisBallSport, data.First().ClassTags.ElementAt(1));
        }

        [Test]
        public async Task Select_JoinOnNonTable_ReturnsCorrectValues1()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .InnerJoin<PersonClass>(nameof(PersonClass), result => result.PersonClasses)
                    .On((q, c) => c.ClassId == Data.Classes.Archery.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).Person);

            foreach (var person in data)
            {
                Assert.AreEqual(1, person.PersonClasses.Count());
                Assert.AreEqual(Data.PersonClasses.JohnArchery, person.PersonClasses.First());
            }
        }

        [Test]
        public async Task Select_JoinOnNonTable_ReturnsCorrectValues2()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .InnerJoin<PersonClass>(nameof(PersonClass), result => result.PersonClasses)
                    .On((q, c) => c.ClassId == Data.Classes.Tennis.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).Person);

            foreach (var person in data)
            {
                Assert.AreEqual(2, person.PersonClasses.Count());
                Assert.AreEqual(Data.PersonClasses.JohnTennis, person.PersonClasses.First());
                Assert.AreEqual(Data.PersonClasses.MaryTennis, person.PersonClasses.ElementAt(1));
            }
        }

        [Test]
        public async Task Select_JoinTableAndNonTable_ReturnsCorrectValues()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .InnerJoin<PersonClass>(nameof(PersonClass), result => result.PersonClasses)
                    .On((q, c) => q.Person.Id == c.PersonId && c.ClassId == Data.Classes.Tennis.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).Person);

            Assert.AreEqual(1, data.First().PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis, data.First().PersonClasses.First());
            
            Assert.AreEqual(1, data.ElementAt(1).PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.ElementAt(1).PersonClasses.First());
        }
    }
}