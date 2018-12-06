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
using SqlDsl.ObjectBuilders;

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

        class Cls1
        {
            public string name;
            public Cls2[] classes;
        }

        class Cls2
        {
            public string name;
            public Cls3[] tags1;
            public Cls3[] tags2;
        }

        class Cls3
        {
            public string tagName;
        }

        [Test]
        public async Task CountObjectGraphAllocations_ForProperties()
        {
            // arrange
            // act
            var tt= await FullyJoinedQuery<object>()
                .Map(x => new Cls1
                {
                    name = x.ThePerson.Name,
                    classes = x.Classes
                        .Select(cl => new Cls2
                        {
                            name = cl.Name,
                            tags1 = x.Tags
                                .Select(z => new Cls3 { tagName = z.Name })
                                .ToArray(),
                            tags2 = x.Tags
                                .Select(z => new Cls3 { tagName = z.Name })
                                .ToArray()
                        })
                        .ToArray()
                })
                .ToArrayAsync(Executor, null, logger: Logger);

            // assert
            var debugCount = Logger.DebugMessages
                .Where(m => m.Contains(((int)LogMessages.CreatedObjectGraphAllocation).ToString()))
                .Count();

            Assert.AreEqual(3, debugCount, "3 objects represents 3 levels of properties");
        }

        [Test]
        public async Task CountObjectGraphAllocations_ForConstructorArgs()
        {
            // arrange
            // act
            var tt= await FullyJoinedQuery<object>()
                .Map(x => new
                {
                    name = x.ThePerson.Name,
                    classes = x.Classes
                        .Select(cl => new
                        {
                            name = cl.Name,
                            tags1 = x.Tags
                                .Select(z => new { tagName = z.Name })
                                .ToArray(),
                            tags2 = x.Tags
                                .Select(z => new { tagName = z.Name })
                                .ToArray()
                        })
                        .ToArray()
                })
                .ToArrayAsync(Executor, null, logger: Logger);

            // assert
            var debugCount = Logger.DebugMessages
                .Where(m => m.Contains(((int)LogMessages.CreatedObjectGraphAllocation).ToString()))
                .Count();

            Assert.AreEqual(3, debugCount, "3 objects represents 3 levels of properties");
        }

        [Test]
        public async Task SimpleMapReturningEmptyObject()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => new object())
                .ToIEnumerableAsync(Executor, null, logger: Logger);

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
                .ToIEnumerableAsync(Executor, null, logger: Logger);

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
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Name, data.First());
            Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1));
        }
        
        [Test]
        public async Task ReturnOneFromMap()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Where(q => q.PersonClasses.One().ClassId == Data.Classes.Tennis.Id)
                .Map(p => p.PersonClasses.One())
                .ToIEnumerableAsync(Executor, null, logger: Logger);

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
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(2, data.First().Count());
            Assert.AreEqual(1, data.ElementAt(1).Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis, data.First().First());
            Assert.AreEqual(Data.PersonClasses.JohnArchery, data.First().ElementAt(1));
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.ElementAt(1).First());
        }

        [Test]
        public async Task ReturnOneSubPropFromMap()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Where(q => q.PersonClasses.One().ClassId == Data.Classes.Tennis.Id)
                .Map(p => p.PersonClasses.One().ClassId)
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data.First());
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data.ElementAt(1));
        }

        [Test]
        public async Task ReturnOneSubPropFromMap2()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Where(q => q.PersonClasses.One().ClassId == Data.Classes.Tennis.Id)
                .Map(p => p.PersonClasses.Select(x => x.ClassId).One())
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data.First());
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data.ElementAt(1));
        }

        [Test]
        public async Task ReturnMultipleSubPropsFromMap()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.PersonClasses.Select(pc => pc.ClassId))
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(2, data.First().Count());
            Assert.AreEqual(1, data.ElementAt(1).Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data.First().First());
            Assert.AreEqual(Data.PersonClasses.JohnArchery.ClassId, data.First().ElementAt(1));
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data.ElementAt(1).First());
        }

        [Test]
        public async Task MapAndReturnConstant()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => 77)
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First());
            Assert.AreEqual(77, data.ElementAt(1));
        }

        [Test]
        public async Task MapAndReturnArg1()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<int>()
                .Map((p, a) => a)
                .ToIEnumerableAsync(Executor, 77, logger: Logger);

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
        public async Task MapAndReturnArg2()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<AnInt>()
                .Map((p, a) => a.IntValue)
                .ToIEnumerableAsync(Executor, new AnInt { IntValue = 77 }, logger: Logger);

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
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First().Id);
            Assert.AreEqual(77, data.ElementAt(1).Id);
        }

        [Test]
        public async Task MapWithAddition()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.ThePerson.Id + 1)
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Id + 1, data.First());
            Assert.AreEqual(Data.People.Mary.Id + 1, data.ElementAt(1));
        }

        [Test]
        public async Task MapWithAddition_SwappedAround()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => 1 + p.ThePerson.Id)
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Id + 1, data.First());
            Assert.AreEqual(Data.People.Mary.Id + 1, data.ElementAt(1));
        }

        [Test]
        public async Task MapWithAddition2()
        {
            // arrange
            var one = 1;

            // act
            var data = await FullyJoinedQuery<int>()
                .Map((p, a) => p.ThePerson.Id + one + a)
                .ToIEnumerableAsync(Executor, 10, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Id + 11, data.First());
            Assert.AreEqual(Data.People.Mary.Id + 11, data.ElementAt(1));
        }

        [Test]
        public async Task AdditionInWhere()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<Person>()
                .From()
                .Where(p => p.Id + 1 == Data.People.John.Id + 1)
                .ToListAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] {Data.People.John}, data);
        }

        [Test]
        public async Task AdditionInJoin()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.ThePerson.Id + 1 == pc.PersonId + 1)
                .Where(p => p.ThePerson.Id == Data.People.John.Id)
                .ToListAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] {Data.People.John}, data.Select(d => d.ThePerson));
            CollectionAssert.AreEqual(new [] {Data.PersonClasses.JohnArchery, Data.PersonClasses.JohnTennis}, data.SelectMany(d => d.PersonClasses));
        }

        [Test]
        public async Task MapWithAddition_InComplexObject()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => new
                {
                    pid = p.ThePerson.Id + 1,
                    classes = p.PersonClasses
                        .Select(pc => pc.ClassId + p.Classes.One().Id)
                        .ToList()
                })
                .ToArrayAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Id + 1, data[0].pid);
            CollectionAssert.AreEqual(new [] {Data.Classes.Tennis.Id * 2, Data.Classes.Archery.Id * 2}, data[0].classes);

            Assert.AreEqual(Data.People.Mary.Id + 1, data[1].pid);
            CollectionAssert.AreEqual(new [] {Data.Classes.Tennis.Id * 2}, data[1].classes);
        }
    }
}
