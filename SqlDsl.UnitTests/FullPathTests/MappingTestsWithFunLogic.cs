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

        static Dsl.IQuery<TArg, JoinedQueryClass> FullyJoinedQuery<TArg>()
        {
            return Sql.Query.Sqlite<TArg, JoinedQueryClass>()
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
            var data = await FullyJoinedQuery<object>()
                .Map(p => new object())
                .ExecuteAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(typeof(object), data.First().GetType());
            Assert.AreEqual(typeof(object), data.ElementAt(1).GetType());
        }

        [Test]
        public async Task SimpleMapOn1FullTable()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.ThePerson)
                .ExecuteAsync(Executor, null, logger: Logger);

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
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.ThePerson.Name)
                .ExecuteAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Name, data.First());
            Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1));
        }
        
        [Test]
        [Ignore("TODO")]
        public async Task ReturnOneFromMap()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Where(q => q.PersonClasses.One().ClassId == Data.Classes.Tennis.Id)
                .Map(p => p.PersonClasses.One())
                .ExecuteAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis, data.First());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.ElementAt(1));
        }

        [Test]
        public async Task ReturnMultipleFromMap()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.PersonClasses)
                .ExecuteAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(2, data.First().Count());
            Assert.AreEqual(1, data.ElementAt(1).Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis, data.First().First());
            Assert.AreEqual(Data.PersonClasses.JohnArchery, data.First().ElementAt(1));
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.ElementAt(1).First());
        }

        [Test]
        public async Task ReturnMultipleFromMap2()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.PersonClasses.Select(x => x))
                .ExecuteAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(2, data.First().Count());
            Assert.AreEqual(1, data.ElementAt(1).Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis, data.First().First());
            Assert.AreEqual(Data.PersonClasses.JohnArchery, data.First().ElementAt(1));
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.ElementAt(1).First());
        }

        [Test]
        [Ignore("TODO")]
        public async Task ReturnOneSubPropFromMap()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Where(q => q.PersonClasses.One().ClassId == Data.Classes.Tennis.Id)
                .Map(p => p.PersonClasses.One().ClassId)
                .ExecuteAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data.First());
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data.ElementAt(1));
        }

        [Test]
        [Ignore("TODO")]
        public async Task ReturnMultipleSubPropsFromMap()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Where(q => q.PersonClasses.One().ClassId == Data.Classes.Tennis.Id)
                .Map(p => p.PersonClasses.Select(pc => pc.ClassId))
                .ExecuteAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(1, data.First().Count());
            Assert.AreEqual(1, data.ElementAt(1).Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data.First().First());
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data.ElementAt(1).First());
        }

        [Test]
        [Ignore("TODO")]
        public async Task SimpleMapOn1Table2()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.ThePerson)
                .ExecuteAsync(Executor, null, logger: Logger);

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
            var data = await FullyJoinedQuery<object>()
                .Map(p => 77)
                .ExecuteAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First());
            Assert.AreEqual(77, data.ElementAt(1));
        }

        [Test]
        [Ignore("TODO")]
        public async Task MapAndReturnArg1()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<int>()
                .Map((p, a) => a)
                .ExecuteAsync(Executor, 77, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First());
            Assert.AreEqual(77, data.ElementAt(1));
        }

        class AnInt
        {
            public int IntValue;
        }

        [Test]
        [Ignore("TODO")]
        public async Task MapAndReturnArg2()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<AnInt>()
                .Map((p, a) => a)
                .ExecuteAsync(Executor,new AnInt { IntValue = 77 }, logger: Logger);

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
            var data = await FullyJoinedQuery<object>()
                .Map(p => new Person { Id = 77 })
                .ExecuteAsync(Executor, null, logger: Logger);

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
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.ThePerson.Id + 1)
                .ExecuteAsync(Executor, null, logger: Logger);

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
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.ThePerson.Id + one)
                .ExecuteAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Id + 1, data.First());
            Assert.AreEqual(Data.People.Mary.Id + 1, data.ElementAt(1));
        }
    }
}
