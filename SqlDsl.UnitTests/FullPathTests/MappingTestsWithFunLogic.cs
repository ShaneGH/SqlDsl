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
    public class MappingTestsWithFunLogic : FullPathTestBase
    {
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public List<PersonClass> PersonClasses { get; set; }
            public List<Class> Classes { get; set; }
            public List<ClassTag> ClassTags { get; set; }
            public List<Tag> Tags { get; set; }
        }

        static Dsl.IQuery<JoinedQueryClass> FullyJoinedQuery()
        {
            return Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<Class>(q => q.Classes)
                    .On((q, c) => q.PersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.ClassTags)
                    .On((q, ct) => q.Classes.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.Tags)
                    .On((q, t) => q.ClassTags.One().TagId == t.Id);
        }

        [Test]
        public async Task SimpleMapReturningEmptyObject()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Map(p => new object())
                .ExecuteAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(typeof(object), data.First().GetType());
            Assert.AreEqual(typeof(object), data.ElementAt(1).GetType());
        }

        [Test]
        [Ignore("TODO")]
        public async Task SimpleMapOn1FullTable()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Map(p => p.ThePerson)
                .ExecuteAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1));
        }

        [Test]
        public async Task SimpleMapOn1Table()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Map(p => p.ThePerson.Name)
                .ExecuteAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Name, data.First());
            Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1));
        }

        // TODO: need to investigate the how arrays, One and Select impact results
        // Map(x => 2)
        // Map((x, a) => a)
        // Map((x, a) => a.Value)
        // Map((x, a) => someVar)
        // Map(x => x.PersonClasses)
        // Map(x => x.PersonClasses).One()
        // Map(x => x.PersonClasses).One().PersonId
        // Map(x => x.PersonClasses).Select(x => x.PersonId)

        [Test]
        [Ignore("TODO")]
        public async Task SimpleMapOn1Table2()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Map(p => p.ThePerson)
                .ExecuteAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1));
        }

        [Test]
        [Ignore("TODO")]
        public async Task MapAndReturnConstant()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Map(p => 77)
                .ExecuteAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First());
            Assert.AreEqual(77, data.ElementAt(1));
        }

        [Test]
        public async Task MapAndReturnMappedConstant()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Map(p => new Person { Id = 77 })
                .ExecuteAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First().Id);
            Assert.AreEqual(77, data.ElementAt(1).Id);
        }

        [Test]
        [Ignore("TODO")]
        public async Task MapWithAddition()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Map(p => p.ThePerson.Id + 1)
                .ExecuteAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Id + 1, data.First());
            Assert.AreEqual(Data.People.Mary.Id + 1, data.ElementAt(1));
        }

        [Test]
        [Ignore("TODO")]
        public async Task MapWithAddition2()
        {
            // arrange
            var one = 1;

            // act
            var data = await FullyJoinedQuery()
                .Map(p => p.ThePerson.Id + one)
                .ExecuteAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Id + 1, data.First());
            Assert.AreEqual(Data.People.Mary.Id + 1, data.ElementAt(1));
        }
    }
}
