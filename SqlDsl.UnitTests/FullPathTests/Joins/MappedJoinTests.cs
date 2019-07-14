using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.Joins
{
    [SqlTestAttribute(SqlType.TSql)]
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
            CollectionAssert.AreEquivalent(new[] { Data.PersonClasses.JohnTennis, Data.PersonClasses.JohnArchery }, john.personClasses);
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
            CollectionAssert.AreEquivalent(new[] { Data.PersonClasses.JohnTennis, Data.PersonClasses.JohnArchery }, john.personClasses);
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
            CollectionAssert.AreEqual(new[] { Data.PersonClasses.JohnArchery, Data.PersonClasses.JohnTennis }, john.personClasses);
        }

        [Test]
        public async Task Join2Levels_1()
        {
            // arrange
            // act
            var john = await TestUtils.FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.TheClasses
                })
                .FirstAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEquivalent(new[] { Data.Classes.Tennis, Data.Classes.Archery }, john.classes);
        }

        [Test]
        public async Task Join2Levels_2()
        {
            // arrange
            // act
            var john = await TestUtils.FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.TheClasses
                })
                .FirstAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEquivalent(new[] { Data.Classes.Tennis, Data.Classes.Archery }, john.classes);
        }

        [Test]
        public async Task Join2Levels_3()
        {
            // arrange
            // act
            var john = await TestUtils.FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.TheClasses
                })
                .FirstAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEquivalent(new[] { Data.Classes.Tennis, Data.Classes.Archery }, john.classes);
        }

        [Test]
        public void Join2Levels_WithSelect_1()
        {
            // arrange
            // act
            var john = TestUtils.FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.ThePersonClasses
                        .Select(pc => new 
                        {
                            cls = q.TheClasses.One()
                        })
                })
                .ToIEnumerable(Executor, logger: Logger)
                .Select(x => x.classes)
                .First();

            // assert
            CollectionAssert.AreEquivalent(new []
            {
                new { cls = Data.Classes.Tennis },
                new { cls = Data.Classes.Archery }
            }, john);
        }

        [Test]
        public void Join2Levels_WithSelect_2()
        {
            // arrange
            base.PrintStatusOnFailure = false;
            
            // act
            var john = TestUtils.FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new 
                {
                    classes = q.ThePersonClasses
                        .Select(pc => new 
                        {
                            cls = q.TheClasses.One()
                        })
                })
                .ToIEnumerable(Executor, logger: Logger)
                .Select(x => x.classes)
                .First();

            // assert
            CollectionAssert.AreEquivalent(new []
            {
                new { cls = Data.Classes.Tennis },
                new { cls = Data.Classes.Archery }
            }, john);
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
            CollectionAssert.AreEquivalent(new []
            {
                Data.ClassTags.TennisSport,
                Data.ClassTags.TennisBallSport,
                Data.ClassTags.ArcherySport
            }, john.tags.Select(t => t.tag));
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
            CollectionAssert.AreEquivalent(new []
            {
                Data.Classes.Tennis,
                Data.Classes.Tennis,
                Data.Classes.Archery
            }, data[0].classesWhichAreTagged.Select(c => c.cls));
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
            CollectionAssert.AreEquivalent(new []
            {
                Data.Classes.Tennis,
                Data.Classes.Tennis,
                Data.Classes.Archery
            }, data[0].tags.Select(t => t.cls));
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
            CollectionAssert.AreEquivalent(new []
            {
                Data.Classes.Tennis.Name,
                Data.Classes.Tennis.Name,
                Data.Classes.Archery.Name
            }, data[0].classesWithTags.Select(c => c.cls));
        }

        [Test]
        public void Join2Levels_UsesDataFromAbove_3()
        {
            // arrange
            // act
            var john = TestUtils.FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classes = q.TheClasses
                        .Select(c => new 
                        {
                            name = c.Name,
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
                .ToList(Executor, logger: Logger)
                .First();

            // assert
            Assert.AreEqual(2, john.classes.Length);
            var tennis = john.classes.First(x => x.name == Data.Classes.Tennis.Name).data;
            var archery = john.classes.First(x => x.name == Data.Classes.Archery.Name).data;

            CollectionAssert.AreEquivalent(new []
            {
                new { tagName = Data.Tags.Sport.Name, className = Data.Classes.Tennis.Name, personName = Data.People.John.Name },
                new { tagName = Data.Tags.BallSport.Name, className = Data.Classes.Tennis.Name, personName = Data.People.John.Name }
            }, tennis);

            CollectionAssert.AreEquivalent(new []
            {
                new { tagName = Data.Tags.Sport.Name, className = Data.Classes.Archery.Name, personName = Data.People.John.Name }
            }, archery);
        }
    }
}
