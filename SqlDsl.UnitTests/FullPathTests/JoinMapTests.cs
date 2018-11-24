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
using SqlDsl.Dsl;

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class JoinMapTests : FullPathTestBase
    {
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public List<PersonClass> PersonClasses { get; set; }
            public List<Class> Classes { get; set; }
            public List<ClassTag> ClassTags { get; set; }
            public List<Tag> Tags { get; set; }
        }

        IQuery<JoinedQueryClass> FullyJoinedQuery()
        {
            return Sql.Query.Sqlite<JoinedQueryClass>()
                .From(result => result.ThePerson)
                .LeftJoin<PersonClass>(result => result.PersonClasses)
                    .On((r, pc) => r.ThePerson.Id == pc.PersonId)
                .LeftJoin<Class>(result => result.Classes)
                    .On((r, pc) => r.PersonClasses.One().ClassId == pc.Id)
                .LeftJoin<ClassTag>(result => result.ClassTags)
                    .On((r, pc) => r.Classes.One().Id == pc.ClassId)
                .LeftJoin<Tag>(result => result.Tags)
                    .On((r, pc) => r.ClassTags.One().TagId == pc.Id);
        }

        [Test]
        public async Task Join1Level_1()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    personClasses = q.PersonClasses
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            CollectionAssert.AreEqual(new[] { Data.PersonClasses.JohnTennis, Data.PersonClasses.JohnArchery }, john.personClasses);
        }

        [Test]
        public async Task Join1Level_2()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    personClasses = q.Joined(q.PersonClasses)
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            CollectionAssert.AreEqual(new[] { Data.PersonClasses.JohnTennis, Data.PersonClasses.JohnArchery }, john.personClasses);
        }

        [Test]
        public void Join1Level_2_Invalid()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(InvalidOperationException), () =>
                FullyJoinedQuery()
                    .Map(q => new
                    {
                        personClasses = q.Joined(q.Classes)
                    })
                    .ToIEnumerableAsync(Executor, logger: Logger));
        }

        [Test]
        public async Task Join1Level_3()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    personClasses = q.ThePerson.Joined(q.PersonClasses)
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            CollectionAssert.AreEqual(new[] { Data.PersonClasses.JohnTennis, Data.PersonClasses.JohnArchery }, john.personClasses);
        }

        [Test]
        public void Join1Level_3_Invalid()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(InvalidOperationException), () =>
                FullyJoinedQuery()
                    .Map(q => new
                    {
                        personClasses = q.ThePerson.Joined(q.Classes)
                    })
                    .ToIEnumerableAsync(Executor, logger: Logger));
        }

        [Test]
        public async Task Join2Levels_1()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.PersonClasses.Joined(q.Classes)
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            CollectionAssert.AreEqual(new[] { Data.Classes.Tennis, Data.Classes.Archery }, john.classes);
        }

        [Test]
        public void Join2Levels_1_Invalid()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(InvalidOperationException), () =>
                FullyJoinedQuery()
                    .Map(q => new
                    {
                        classes = q.PersonClasses.Joined(q.Tags)
                    })
                    .ToIEnumerableAsync(Executor, logger: Logger));
        }

        [Test]
        public async Task Join2Levels_2()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.Joined(q.PersonClasses).Joined(q.Classes)
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            CollectionAssert.AreEqual(new[] { Data.Classes.Tennis, Data.Classes.Archery }, john.classes);
        }

        [Test]
        public async Task Join2Levels_3()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.ThePerson.Joined(q.PersonClasses).Joined(q.Classes)
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            CollectionAssert.AreEqual(new[] { Data.Classes.Tennis, Data.Classes.Archery }, john.classes);
        }

        [Test]
        [Ignore("TODO")]
        public async Task Join2Levels_WithSelect_1()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.PersonClasses
                        .Select(pc => new 
                        {
                            classes = q.Classes.ToArray()
                        })
                        .ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            Assert.AreEqual(2, john.classes.Length);
            CollectionAssert.AreEqual(new[] { Data.Classes.Tennis }, john.classes[0].classes);
            CollectionAssert.AreEqual(new[] { Data.Classes.Archery }, john.classes[1].classes);
        }

        [Test]
        [Ignore("TODO")]
        public void Join2Levels_WithSelect_1_Invalid()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(InvalidOperationException), () =>
                FullyJoinedQuery()
                    .Map(q => new
                    {
                        classes = q.PersonClasses
                            .Select(pc => new 
                            {
                                classes = q.ClassTags
                            })
                            .ToArray()
                    })
                    .ToIEnumerableAsync(Executor, logger: Logger));
        }

        [Test]
        public async Task Join2Levels_WithSelect_2()
        {
            // arrange
            base.PrintStatusOnFailure = false;
            
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new 
                {
                    classes = q.PersonClasses
                        .Select(pc => new 
                        {
                            classes = pc.Joined(q.Classes).ToArray()
                        })
                        .ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            Assert.AreEqual(2, john.classes.Length);
            CollectionAssert.AreEqual(new[] { Data.Classes.Tennis }, john.classes[0].classes);
            CollectionAssert.AreEqual(new[] { Data.Classes.Archery }, john.classes[1].classes);
        }

        [Test]
        [Ignore("TODO")]
        public async Task Join2Levels_WithSkippedLevel_1()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    tags = q.PersonClasses
                        .Joined(q.Classes)
                        .Joined(q.ClassTags)
                        .Select(tag => new 
                        {
                            tag = tag
                        })
                        .ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            Assert.AreEqual(3, john.tags.Length);
            Assert.AreEqual(Data.Tags.BallSport, john.tags[0].tag);
            Assert.AreEqual(Data.Tags.Sport, john.tags[1].tag);
            Assert.AreEqual(Data.Tags.Sport, john.tags[2].tag);
        }

        class Cls1
        {
            public Cls2[] tags;
        }

        class Cls2
        {
            public Class cls;
        }

        [Test]
        public void Join2Levels_UsesDataFromAbove_1_1()
        {
            // arrange
            // act
            var data = FullyJoinedQuery()
                .Where(q => q.ThePerson.Id == Data.People.John.Id)
                .Map(q => new Cls1
                {
                    tags = q.PersonClasses
                        .Joined(q.Classes)
                        .Joined(q.ClassTags)
                        .Joined(q.Tags)
                        .Select(tag => new Cls2
                        {
                            cls = q.Classes.One()
                        })
                        .ToArray()
                })
                .ToList(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            Assert.AreEqual(3, data[0].tags.Length);
            Assert.AreEqual(Data.Classes.Tennis, data[0].tags[0].cls);
            Assert.AreEqual(Data.Classes.Tennis, data[0].tags[1].cls);
            Assert.AreEqual(Data.Classes.Archery, data[0].tags[2].cls);
        }

        [Test]
        public void Join2Levels_UsesDataFromAbove_1_3()
        {
            // arrange
            // act
            var data = FullyJoinedQuery()
                .Where(q => q.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    tags = q.PersonClasses
                        .Joined(q.Classes)
                        .Joined(q.ClassTags)
                        .Joined(q.Tags)
                        .Select(tag => new
                        {
                            cls = q.Classes.One()
                        })
                        .ToArray()
                })
                .ToList(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            Assert.AreEqual(3, data[0].tags.Length);
            Assert.AreEqual(Data.Classes.Tennis, data[0].tags[0].cls);
            Assert.AreEqual(Data.Classes.Tennis, data[0].tags[1].cls);
            Assert.AreEqual(Data.Classes.Archery, data[0].tags[2].cls);
        }

        [Test]
        public void Join2Levels_UsesDataFromAbove_2()
        {
            // arrange
            // act
            var data = FullyJoinedQuery()
                .Where(q => q.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    tags = q.PersonClasses
                        .Joined(q.Classes)
                        .Joined(q.ClassTags)
                        .Select(tag => new 
                        {
                            cls = q.Classes.One().Name
                        })
                        .ToArray()
                })
                .ToList(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            Assert.AreEqual(3, data[0].tags.Length);
            Assert.AreEqual(Data.Classes.Tennis.Name, data[0].tags[0].cls);
            Assert.AreEqual(Data.Classes.Tennis.Name, data[0].tags[1].cls);
            Assert.AreEqual(Data.Classes.Archery.Name, data[0].tags[2].cls);
        }

        [Test]
        public async Task Join2Levels_UsesDataFromAbove_3()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.PersonClasses
                        .Joined(q.Classes)
                        .Select(c => new 
                        {
                            data = c
                                .Joined(q.ClassTags)
                                .Joined(q.Tags)
                                .Select(t => new 
                                {
                                    tagName = t.Name,
                                    className = c.Name,
                                    personName = q.ThePerson.Name
                                })
                                .ToArray()
                        })
                        .ToArray()
                })
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();

            Assert.AreEqual(2, john.classes.Length);
            Assert.AreEqual(2, john.classes[0].data.Length);
            Assert.AreEqual(1, john.classes[1].data.Length);

            var tag1 = new { tagName = Data.Tags.Sport.Name, className = Data.Classes.Tennis.Name, personName = Data.People.John.Name };
            Assert.AreEqual(tag1, john.classes[0].data[0]);

            var tag2 = new { tagName = Data.Tags.BallSport.Name, className = Data.Classes.Tennis.Name, personName = Data.People.John.Name };
            Assert.AreEqual(tag2, john.classes[0].data[1]);

            var tag3 = new { tagName = Data.Tags.Sport.Name, className = Data.Classes.Archery.Name, personName = Data.People.John.Name };
            Assert.AreEqual(tag3, john.classes[1].data[0]);
        }
    }
}
