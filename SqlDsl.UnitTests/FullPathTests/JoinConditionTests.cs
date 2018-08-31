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
    public class JoinConditionTests
    {
        class QueryClass
        {
            public Person Person { get; set; }
            public IEnumerable<PersonClass> PersonClasses { get; set; }
            public IEnumerable<ClassTag> ClassTags { get; set; }
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

        void AssertSelect1SimpleJoin(IEnumerable<QueryClass> data)
        {
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).Person);

            Assert.AreEqual(2, data.First().PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis, data.First().PersonClasses.ElementAt(0));
            Assert.AreEqual(Data.PersonClasses.JohnArchery, data.First().PersonClasses.ElementAt(1));

            Assert.AreEqual(1, data.ElementAt(1).PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.ElementAt(1).PersonClasses.ElementAt(0));
        }

        [Test]
        public async Task Select1SimpleJoin()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass>()
                .From(nameof(Person), result => result.Person)
                .InnerJoin<PersonClass>(nameof(PersonClass), result => result.PersonClasses)
                    .On((q, c) => q.Person.Id == c.PersonId)
                .ExecuteAsync(new SqliteExecutor(Connection));

            // assert
            AssertSelect1SimpleJoin(data);
        }

        [Test]
        public async Task Select2Joins_DoesNotDuplicateRecords()
        {
            var testEx = new TestExecutor(new SqliteExecutor(Connection));

            try
            {
                // arrange
                // act
                var data = await Sql.Query.Sqlite<QueryClass>()
                    .From(nameof(Person), result => result.Person)
                    .InnerJoin<PersonClass>(nameof(PersonClass), result => result.PersonClasses)
                        .On((q, c) => q.Person.Id == c.PersonId)
                    .InnerJoin<ClassTag>(nameof(ClassTag), result => result.ClassTags)
                        .On((q, c) => Sql.One(q.PersonClasses).ClassId == c.ClassId)
                    .ExecuteAsync(testEx);

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
            catch
            {
                testEx.PrintSqlStatements();
                throw;
            }
        }
    }
}