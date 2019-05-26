using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.Joins
{
    [SqlTestAttribute(SqlType.MySql)]
    [SqlTestAttribute(SqlType.Sqlite)]
    public class MappedJoinTests : FullPathTestBase
    {
        public MappedJoinTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        [Test]
        public async Task Join1Level_1()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    personClasses = q.ThePersonClasses
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
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    personClasses = q.ThePersonClasses
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            CollectionAssert.AreEqual(new[] { Data.PersonClasses.JohnTennis, Data.PersonClasses.JohnArchery }, john.personClasses);
        }

        [Test]
        public async Task Join1Level_3()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    personClasses = q.ThePersonClasses
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            CollectionAssert.AreEqual(new[] { Data.PersonClasses.JohnTennis, Data.PersonClasses.JohnArchery }, john.personClasses);
        }

        [Test]
        public async Task Join2Levels_1()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.TheClasses
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            CollectionAssert.AreEqual(new[] { Data.Classes.Tennis, Data.Classes.Archery }, john.classes);
        }

        [Test]
        public async Task Join2Levels_2()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.TheClasses
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
            var data = await TestUtils.FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.TheClasses
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            CollectionAssert.AreEqual(new[] { Data.Classes.Tennis, Data.Classes.Archery }, john.classes);
        }

        [Test]
        public async Task Join2Levels_WithSelect_1()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.ThePersonClasses
                        .Select(pc => new 
                        {
                            classes = q.TheClasses.ToArray()
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
        public async Task Join2Levels_WithSelect_2()
        {
            // arrange
            base.PrintStatusOnFailure = false;
            
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new 
                {
                    classes = q.ThePersonClasses
                        .Select(pc => new 
                        {
                            classes = q.TheClasses.ToArray()
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
        public async Task Join2Levels_WithSkippedLevel_1()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    tags = q.TheClassTags
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
            Assert.AreEqual(Data.ClassTags.TennisSport, john.tags[0].tag);
            Assert.AreEqual(Data.ClassTags.TennisBallSport, john.tags[1].tag);
            Assert.AreEqual(Data.ClassTags.ArcherySport, john.tags[2].tag);
        }

        class Cls1
        {
            public Cls2[] classesWhichAreTagged;
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
            var data = TestUtils.FullyJoinedQuery(SqlType)
                .Where(q => q.ThePerson.Id == Data.People.John.Id)
                .Map(q => new Cls1
                {
                    classesWhichAreTagged = q.TheTags
                        .Select(tag => new Cls2
                        {
                            cls = q.TheClasses.One()
                        })
                        .ToArray()
                })
                .ToList(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            Assert.AreEqual(3, data[0].classesWhichAreTagged.Length);
            Assert.AreEqual(Data.Classes.Tennis, data[0].classesWhichAreTagged[0].cls);
            Assert.AreEqual(Data.Classes.Tennis, data[0].classesWhichAreTagged[1].cls);
            Assert.AreEqual(Data.Classes.Archery, data[0].classesWhichAreTagged[2].cls);
        }

        [Test]
        public void Join2Levels_UsesDataFromAbove_1_3()
        {
            // arrange
            // act
            var data = TestUtils.FullyJoinedQuery(SqlType)
                .Where(q => q.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    tags = q.TheTags
                        .Select(tag => new
                        {
                            cls = q.TheClasses.One()
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
            var data = TestUtils.FullyJoinedQuery(SqlType)
                .Where(q => q.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classesWithTags = q.TheClassTags
                        .Select(tag => new 
                        {
                            cls = q.TheClasses.One().Name
                        })
                        .ToArray()
                })
                .ToList(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            Assert.AreEqual(3, data[0].classesWithTags.Length);
            Assert.AreEqual(Data.Classes.Tennis.Name, data[0].classesWithTags[0].cls);
            Assert.AreEqual(Data.Classes.Tennis.Name, data[0].classesWithTags[1].cls);
            Assert.AreEqual(Data.Classes.Archery.Name, data[0].classesWithTags[2].cls);
        }

        [Test]
        public async Task Join2Levels_UsesDataFromAbove_3()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.TheClasses
                        .Select(c => new 
                        {
                            data = q.TheTags
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
