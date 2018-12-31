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
    public class MappingTests : FullPathTestBase
    {
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public List<PersonClass> ThePersonClasses { get; set; }
            public List<Class> TheClasses { get; set; }
            public List<ClassTag> TheClassTags { get; set; }
            public List<Tag> TheTags { get; set; }
        }

        static Dsl.IQuery<JoinedQueryClass> FullyJoinedQuery()
        {
            return Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id);
        }

        class DeepJoinedQueryClass
        {
            public JoinedQueryClass Query { get; set; }
        }

        static Dsl.IQuery<DeepJoinedQueryClass> DeepFullyJoinedQuery()
        {
            return Sql.Query.Sqlite<DeepJoinedQueryClass>()
                .From<Person>(x => x.Query.ThePerson)
                .InnerJoin<PersonClass>(q => q.Query.ThePersonClasses)
                    .On((q, pc) => q.Query.ThePerson.Id == pc.PersonId)
                .InnerJoin<Class>(q => q.Query.TheClasses)
                    .On((q, c) => q.Query.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.Query.TheClassTags)
                    .On((q, ct) => q.Query.TheClasses.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.Query.TheTags)
                    .On((q, t) => q.Query.TheClassTags.One().TagId == t.Id);
        }

        class SimpleMapClass
        {
            public string TheName { get; set; }
            public SimpleMapClass Inner { get; set; }
            public IEnumerable<long> TheClassIds { get; set; }
            public IEnumerable<long> TheClassTagIds { get; set; }
        }

        [Test]
        public async Task SimpleMapOn1Table()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<Person>()
                .Map(p => new SimpleMapClass
                { 
                    TheName = p.Name,
                    Inner = new SimpleMapClass
                    {
                        TheName = p.Name
                    }
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Name, data.First().TheName);
            Assert.AreEqual(Data.People.John.Name, data.First().Inner.TheName);
            Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1).TheName);
            Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1).Inner.TheName);
        }

        class MapComplexObjectType1
        {
            public string ThePersonName;
            public Person ThePerson;
        }

        [Test]
        public async Task MapComplexObject1()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<Person>()
                .Map(p => new MapComplexObjectType1
                { 
                    ThePerson = p
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).ThePerson);
        }

        [Test]
        public async Task MapComplexObject2()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .Map(p => new MapComplexObjectType1
                { 
                    ThePersonName = p.ThePerson.Name,
                    ThePerson = p.ThePerson
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Name, data.First().ThePersonName);
            Assert.AreEqual(Data.People.John, data.First().ThePerson);

            Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1).ThePersonName);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).ThePerson);
        }

        class DeepSingleClass
        {
            public Person ThePerson { get; set; }
            public DeepSingleClass Inner { get; set; }
        }

        [Test]
        public async Task MapComplexObject3()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<DeepSingleClass>()
                .From<Person>(x => x.Inner.Inner.ThePerson)
                .Map(p => new MapComplexObjectType1
                { 
                    ThePersonName = p.Inner.Inner.ThePerson.Name,
                    ThePerson = p.Inner.Inner.ThePerson
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Name, data.First().ThePersonName);
            Assert.AreEqual(Data.People.John, data.First().ThePerson);

            Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1).ThePersonName);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).ThePerson);
        }

        class MapComplexObjectType2
        {
            public string PersonName;
            public Cls1[] TheClasses;

            public class Cls1
            {
                public string TheName;
                public Tag[] TheTags;
            }
        }

        [Test]
        public async Task MapComplexObject4()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Map(p => new MapComplexObjectType2
                { 
                    PersonName = p.ThePerson.Name,
                    TheClasses = p.TheClasses
                        .Select(c => new MapComplexObjectType2.Cls1
                        {
                            TheName = c.Name,
                            TheTags = p.TheTags
                                .ToArray()
                        })
                        .ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            var john = data.First();
            var mary = data.ElementAt(1);

            Assert.AreEqual(Data.People.John.Name, john.PersonName);            
            Assert.AreEqual(2, john.TheClasses.Length);

            Assert.AreEqual(Data.Classes.Tennis.Name, john.TheClasses[0].TheName);
            Assert.AreEqual(2, john.TheClasses[0].TheTags.Length);

            Assert.AreEqual(Data.Tags.Sport, john.TheClasses[0].TheTags[0]);
            Assert.AreEqual(Data.Tags.BallSport, john.TheClasses[0].TheTags[1]);

            Assert.AreEqual(Data.Classes.Archery.Name, john.TheClasses[1].TheName);
            Assert.AreEqual(1, john.TheClasses[1].TheTags.Length);

            Assert.AreEqual(Data.Tags.Sport, john.TheClasses[1].TheTags[0]);
            

            Assert.AreEqual(Data.People.Mary.Name, mary.PersonName);            
            Assert.AreEqual(1, mary.TheClasses.Length);

            Assert.AreEqual(Data.Classes.Tennis.Name, mary.TheClasses[0].TheName);
            Assert.AreEqual(2, mary.TheClasses[0].TheTags.Length);

            Assert.AreEqual(Data.Tags.Sport, mary.TheClasses[0].TheTags[0]);
            Assert.AreEqual(Data.Tags.BallSport, mary.TheClasses[0].TheTags[1]);
        }

        [Test]
        public async Task MapComplexObject5()
        {
            // arrange
            // act
            var data = await DeepFullyJoinedQuery()
                .Map(p => new MapComplexObjectType2
                { 
                    PersonName = p.Query.ThePerson.Name,
                    TheClasses = p.Query.TheClasses
                        .Select(c => new MapComplexObjectType2.Cls1
                        {
                            TheName = c.Name,
                            TheTags = p.Query.TheTags
                                .ToArray()
                        })
                        .ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            var john = data.First();
            var mary = data.ElementAt(1);

            Assert.AreEqual(Data.People.John.Name, john.PersonName);            
            Assert.AreEqual(2, john.TheClasses.Length);

            Assert.AreEqual(Data.Classes.Tennis.Name, john.TheClasses[0].TheName);
            Assert.AreEqual(2, john.TheClasses[0].TheTags.Length);

            Assert.AreEqual(Data.Tags.Sport, john.TheClasses[0].TheTags[0]);
            Assert.AreEqual(Data.Tags.BallSport, john.TheClasses[0].TheTags[1]);

            Assert.AreEqual(Data.Classes.Archery.Name, john.TheClasses[1].TheName);
            Assert.AreEqual(1, john.TheClasses[1].TheTags.Length);

            Assert.AreEqual(Data.Tags.Sport, john.TheClasses[1].TheTags[0]);
            

            Assert.AreEqual(Data.People.Mary.Name, mary.PersonName);            
            Assert.AreEqual(1, mary.TheClasses.Length);

            Assert.AreEqual(Data.Classes.Tennis.Name, mary.TheClasses[0].TheName);
            Assert.AreEqual(2, mary.TheClasses[0].TheTags.Length);

            Assert.AreEqual(Data.Tags.Sport, mary.TheClasses[0].TheTags[0]);
            Assert.AreEqual(Data.Tags.BallSport, mary.TheClasses[0].TheTags[1]);
        }

        [Test]
        public async Task MapOnTableWith1JoinedTable()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .Where(q => q.ThePerson.Id == 1)
                .Map(p => new SimpleMapClass
                { 
                    TheName = p.ThePerson.Name,
                    TheClassIds = p.ThePersonClasses.Select(c => c.ClassId)
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());

            Assert.AreEqual(Data.People.John.Name, data.First().TheName);
            
            Assert.AreEqual(2, data.First().TheClassIds.Count());
            Assert.Contains(Data.Classes.Tennis.Id, data.First().TheClassIds.ToList());
            Assert.Contains(Data.Classes.Archery.Id, data.First().TheClassIds.ToList());
        }

        [Test]
        public async Task MapOnTableWith2JoinedTables()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.ThePersonClasses.One().ClassId == ct.ClassId)
                .Where(q => q.ThePerson.Id == 1)
                .Map(p => new SimpleMapClass
                { 
                    TheName = p.ThePerson.Name,
                    TheClassIds = p.ThePersonClasses.Select(c => c.ClassId),
                    TheClassTagIds = p.TheClassTags.Select(c => c.TagId)
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());

            var john = data.First();
            Assert.AreEqual(Data.People.John.Name, john.TheName);
            
            Assert.AreEqual(2, john.TheClassIds.Count());
            Assert.Contains(Data.Classes.Tennis.Id, john.TheClassIds.ToList());
            Assert.Contains(Data.Classes.Archery.Id, john.TheClassIds.ToList());
            
            Assert.AreEqual(3, john.TheClassTagIds.Count());
            Assert.AreEqual(2, john.TheClassTagIds.Where(x => x == Data.Tags.Sport.Id).Count());
            Assert.AreEqual(1, john.TheClassTagIds.Where(x => x == Data.Tags.BallSport.Id).Count());
        }

        [Test]
        public async Task MapOnTableWith1JoinedTable_IgnorePrimaryTable()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .Where(q => q.ThePerson.Id == 1)
                .Map(p => new SimpleMapClass
                { 
                    TheClassIds = p.ThePersonClasses.Select(c => c.ClassId)
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            
            Assert.AreEqual(2, data.First().TheClassIds.Count());
            Assert.Contains(Data.Classes.Tennis.Id, data.First().TheClassIds.ToList());
            Assert.Contains(Data.Classes.Archery.Id, data.First().TheClassIds.ToList());
        }

        class JoinedMapClass
        {
            public string TheName { get; set; }
            public IEnumerable<string> TheClassNames { get; set; }
            public IEnumerable<string> TheTagNames { get; set; }
            public List<string> TheClassNamesList { get; set; }
            public string[] TheClassNamesArray { get; set; }
        }

        static void AssertMapOnTableWith2JoinedTables(IEnumerable<JoinedMapClass> data)
        {
            Assert.AreEqual(2, data.Count());

            var john = data.First();
            var mary = data.ElementAt(1);

            // John
            Assert.AreEqual(Data.People.John.Name, john.TheName);
            
            Assert.AreEqual(2, john.TheClassNames.Count());
            Assert.AreEqual(Data.Classes.Tennis.Name, john.TheClassNames.ElementAt(0));
            Assert.AreEqual(Data.Classes.Archery.Name, john.TheClassNames.ElementAt(1));
            Assert.AreEqual(2, john.TheClassNamesList.Count());
            Assert.AreEqual(Data.Classes.Tennis.Name, john.TheClassNamesList.ElementAt(0));
            Assert.AreEqual(Data.Classes.Archery.Name, john.TheClassNamesList.ElementAt(1));
            Assert.AreEqual(2, john.TheClassNamesArray.Count());
            Assert.AreEqual(Data.Classes.Tennis.Name, john.TheClassNamesArray.ElementAt(0));
            Assert.AreEqual(Data.Classes.Archery.Name, john.TheClassNamesArray.ElementAt(1));

            Assert.AreEqual(3, john.TheTagNames.Count(), john.TheTagNames.JoinString(","));
            Assert.AreEqual(Data.Tags.Sport.Name, john.TheTagNames.ElementAt(0));
            Assert.AreEqual(Data.Tags.BallSport.Name, john.TheTagNames.ElementAt(1));
            Assert.AreEqual(Data.Tags.Sport.Name, john.TheTagNames.ElementAt(2));
            
            // Mary
            Assert.AreEqual(Data.People.Mary.Name, mary.TheName);

            Assert.AreEqual(1, mary.TheClassNames.Count());
            Assert.AreEqual(Data.Classes.Tennis.Name, mary.TheClassNames.ElementAt(0));
            Assert.AreEqual(1, mary.TheClassNamesList.Count());
            Assert.AreEqual(Data.Classes.Tennis.Name, mary.TheClassNamesList.ElementAt(0));
            Assert.AreEqual(1, mary.TheClassNamesArray.Count());
            Assert.AreEqual(Data.Classes.Tennis.Name, mary.TheClassNamesArray.ElementAt(0));

            Assert.AreEqual(2, mary.TheTagNames.Count());
            Assert.AreEqual(Data.Tags.Sport.Name, mary.TheTagNames.ElementAt(0));
            Assert.AreEqual(Data.Tags.BallSport.Name, mary.TheTagNames.ElementAt(1));
        }

        [Test]
        public async Task MapOnTableWith2JoinedTables_2()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            var data = await Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id)
                .Map(p => new JoinedMapClass
                { 
                    TheName = p.ThePerson.Name,
                    TheClassNames = p.TheClasses.Select(c => c.Name),
                    TheClassNamesList = p.TheClasses.Select(c => c.Name).ToList(),
                    TheClassNamesArray = p.TheClasses.Select(c => c.Name).ToArray(),
                    TheTagNames = p.TheTags.Select(t => t.Name)
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            AssertMapOnTableWith2JoinedTables(data);
        }

        [Test]
        public async Task MapOnTableWith2JoinedTables_DataIsOnInnerProperty()
        {
            // arrange
            // act
            var data = await DeepFullyJoinedQuery()
                .Map(p => new JoinedMapClass
                { 
                    TheName = p.Query.ThePerson.Name,
                    TheClassNames = p.Query.TheClasses.Select(c => c.Name),
                    TheClassNamesList = p.Query.TheClasses.Select(c => c.Name).ToList(),
                    TheClassNamesArray = p.Query.TheClasses.Select(c => c.Name).ToArray(),
                    TheTagNames = p.Query.TheTags.Select(t => t.Name)
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            AssertMapOnTableWith2JoinedTables(data);
        }

        class SmartJoinedClass3
        {
            public SmartJoinedClass4[] FavouriteClasses;
        }

        class SmartJoinedClass4
        {
            public long[] TagIds;
        }

        [Test]
        public async Task JoinInMap_WithSimpleJoin()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            var data = await FullyJoinedQuery()
                .Map(query => new SmartJoinedClass3
                { 
                    FavouriteClasses = query.TheClasses
                        .Select(c => new SmartJoinedClass4
                        {
                            TagIds = query.TheClassTags
                                .Select(t => t.TagId)
                                .ToArray()
                        })
                        .ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            var john = data.ElementAt(0);
            var mary = data.ElementAt(1);

            // John
            Assert.AreEqual(2, john.FavouriteClasses.Length);
            
            Assert.AreEqual(2, john.FavouriteClasses[0].TagIds.Length);
            Assert.AreEqual(Data.Tags.Sport.Id, john.FavouriteClasses[0].TagIds[0]);
            Assert.AreEqual(Data.Tags.BallSport.Id, john.FavouriteClasses[0].TagIds[1]);
            
            Assert.AreEqual(1, john.FavouriteClasses[1].TagIds.Length);
            Assert.AreEqual(Data.Tags.Sport.Id, john.FavouriteClasses[1].TagIds[0]);
            
            // Mary
            Assert.AreEqual(1, mary.FavouriteClasses.Length);
            
            Assert.AreEqual(2, mary.FavouriteClasses[0].TagIds.Length);
            Assert.AreEqual(Data.Tags.Sport.Id, mary.FavouriteClasses[0].TagIds[0]);
            Assert.AreEqual(Data.Tags.BallSport.Id, mary.FavouriteClasses[0].TagIds[1]);
        }

        [Test]
        public async Task JoinInMap_WithAnonynouseObjects()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            var data = await FullyJoinedQuery()
                .Map(query => new
                { 
                    FavouriteClasses = query.TheClasses
                        .Select(c => new
                        {
                            TagIds = query.TheClassTags
                                .Select(t => t.TagId)
                                .ToArray()
                        })
                        .ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            var john = data.ElementAt(0);
            var mary = data.ElementAt(1);

            // John
            Assert.AreEqual(2, john.FavouriteClasses.Length);
            
            Assert.AreEqual(2, john.FavouriteClasses[0].TagIds.Length);
            Assert.AreEqual(Data.Tags.Sport.Id, john.FavouriteClasses[0].TagIds[0]);
            Assert.AreEqual(Data.Tags.BallSport.Id, john.FavouriteClasses[0].TagIds[1]);
            
            Assert.AreEqual(1, john.FavouriteClasses[1].TagIds.Length);
            Assert.AreEqual(Data.Tags.Sport.Id, john.FavouriteClasses[1].TagIds[0]);
            
            // Mary
            Assert.AreEqual(1, mary.FavouriteClasses.Length);
            
            Assert.AreEqual(2, mary.FavouriteClasses[0].TagIds.Length);
            Assert.AreEqual(Data.Tags.Sport.Id, mary.FavouriteClasses[0].TagIds[0]);
            Assert.AreEqual(Data.Tags.BallSport.Id, mary.FavouriteClasses[0].TagIds[1]);
        }

        class SmartJoinedClass3_1
        {
            public SmartJoinedClass4_1[] FavouriteClasses;
        }

        class SmartJoinedClass4_1
        {
            public long TagId;
        }

        [Test]
        public void JoinInMap_WithSimpleJoin_JoinPropertyIsSingular_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(InvalidOperationException), () => FullyJoinedQuery()
                .Map(query => new SmartJoinedClass3_1
                { 
                    FavouriteClasses = query.TheClasses
                        .Select(c => new SmartJoinedClass4_1
                        {
                            TagId = query.TheClassTags
                                .Select(t => t.TagId)
                                .One()
                        })
                        .ToArray()
                })
                .ToArrayAsync(Executor));
        }

        class SmartJoinedClass3_2
        {
            public SmartJoinedClass3_2 Inner;

            public SmartJoinedClass4_2[] FavouriteClasses;
        }

        class SmartJoinedClass4_2
        {
            public long[] TagIds;
        }

        [Test]
        public async Task JoinInMap_WithSimpleJoin_DeepMap()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            var data = await FullyJoinedQuery()
                .Map(query => new SmartJoinedClass3_2
                { 
                    Inner = new SmartJoinedClass3_2
                    {
                        Inner = new SmartJoinedClass3_2
                        {
                            FavouriteClasses = query.TheClasses
                                .Select(c => new SmartJoinedClass4_2
                                {
                                    TagIds = query.TheClassTags
                                        .Select(t => t.TagId)
                                        .ToArray()
                                })
                                .ToArray()
                        }
                    }
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            var john = data.ElementAt(0).Inner.Inner;
            var mary = data.ElementAt(1).Inner.Inner;

            // John
            Assert.AreEqual(2, john.FavouriteClasses.Length);
            
            Assert.AreEqual(2, john.FavouriteClasses[0].TagIds.Length);
            Assert.AreEqual(Data.Tags.Sport.Id, john.FavouriteClasses[0].TagIds[0]);
            Assert.AreEqual(Data.Tags.BallSport.Id, john.FavouriteClasses[0].TagIds[1]);
            
            Assert.AreEqual(1, john.FavouriteClasses[1].TagIds.Length);
            Assert.AreEqual(Data.Tags.Sport.Id, john.FavouriteClasses[1].TagIds[0]);
            
            // Mary
            Assert.AreEqual(1, mary.FavouriteClasses.Length);
            
            Assert.AreEqual(2, mary.FavouriteClasses[0].TagIds.Length);
            Assert.AreEqual(Data.Tags.Sport.Id, mary.FavouriteClasses[0].TagIds[0]);
            Assert.AreEqual(Data.Tags.BallSport.Id, mary.FavouriteClasses[0].TagIds[1]);
        }

        class SmartJoinedClass1
        {
            public SmartJoinedClass1() { }

            public SmartJoinedClass1(string personName): this() { PersonsName = personName; }

            public string PersonsName;
            public SmartJoinedClass2[] FavouriteClasses;
        }

        class SmartJoinedClass2
        {
            public SmartJoinedClass2() { }

            public SmartJoinedClass2(string className): this() { ClassName = className; }

            public string ClassName;
            public string[] TagNames;
        }

        [Test]
        public async Task JoinInMap_With2LevelJoin()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Map(query => new SmartJoinedClass1
                { 
                    PersonsName = query.ThePerson.Name,
                    FavouriteClasses = query.TheClasses
                        .Select(c => new SmartJoinedClass2
                        {
                            ClassName = c.Name,
                            TagNames = query.TheTags
                                .Select(t => t.Name)
                                .ToArray()
                        })
                        .ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            var john = data.ElementAt(0);
            var mary = data.ElementAt(1);

            // John
            Assert.AreEqual(Data.People.John.Name, john.PersonsName);
            
            Assert.AreEqual(2, john.FavouriteClasses.Length);
            Assert.AreEqual(Data.Classes.Tennis.Name, john.FavouriteClasses[0].ClassName);
            Assert.AreEqual(Data.Classes.Archery.Name, john.FavouriteClasses[1].ClassName);
            
            Assert.AreEqual(2, john.FavouriteClasses[0].TagNames.Length);
            Assert.AreEqual(Data.Tags.Sport.Name, john.FavouriteClasses[0].TagNames[0]);
            Assert.AreEqual(Data.Tags.BallSport.Name, john.FavouriteClasses[0].TagNames[1]);
            
            Assert.AreEqual(1, john.FavouriteClasses[1].TagNames.Length);
            Assert.AreEqual(Data.Tags.Sport.Name, john.FavouriteClasses[1].TagNames[0]);
            
            // Mary
            Assert.AreEqual(Data.People.Mary.Name, mary.PersonsName);
            
            Assert.AreEqual(1, mary.FavouriteClasses.Length);
            Assert.AreEqual(Data.Classes.Tennis.Name, mary.FavouriteClasses[0].ClassName);
            
            Assert.AreEqual(2, mary.FavouriteClasses[0].TagNames.Length);
            Assert.AreEqual(Data.Tags.Sport.Name, mary.FavouriteClasses[0].TagNames[0]);
            Assert.AreEqual(Data.Tags.BallSport.Name, mary.FavouriteClasses[0].TagNames[1]);
        }

        [Test]
        public async Task JoinInMap_WithConstructorArgs()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Map(query => new SmartJoinedClass1(query.ThePerson.Name)
                { 
                    FavouriteClasses = query.TheClasses
                        .Select(c => new SmartJoinedClass2(c.Name)
                        {
                            TagNames = query.TheTags
                                .Select(t => t.Name)
                                .ToArray()
                        })
                        .ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            var john = data.ElementAt(0);
            var mary = data.ElementAt(1);

            // John
            Assert.AreEqual(Data.People.John.Name, john.PersonsName);
            
            Assert.AreEqual(2, john.FavouriteClasses.Length);
            Assert.AreEqual(Data.Classes.Tennis.Name, john.FavouriteClasses[0].ClassName);
            Assert.AreEqual(Data.Classes.Archery.Name, john.FavouriteClasses[1].ClassName);
            
            Assert.AreEqual(2, john.FavouriteClasses[0].TagNames.Length);
            Assert.AreEqual(Data.Tags.Sport.Name, john.FavouriteClasses[0].TagNames[0]);
            Assert.AreEqual(Data.Tags.BallSport.Name, john.FavouriteClasses[0].TagNames[1]);
            
            Assert.AreEqual(1, john.FavouriteClasses[1].TagNames.Length);
            Assert.AreEqual(Data.Tags.Sport.Name, john.FavouriteClasses[1].TagNames[0]);
            
            // Mary
            Assert.AreEqual(Data.People.Mary.Name, mary.PersonsName);
            
            Assert.AreEqual(1, mary.FavouriteClasses.Length);
            Assert.AreEqual(Data.Classes.Tennis.Name, mary.FavouriteClasses[0].ClassName);
            
            Assert.AreEqual(2, mary.FavouriteClasses[0].TagNames.Length);
            Assert.AreEqual(Data.Tags.Sport.Name, mary.FavouriteClasses[0].TagNames[0]);
            Assert.AreEqual(Data.Tags.BallSport.Name, mary.FavouriteClasses[0].TagNames[1]);
        }

        class SmartJoinedClass5
        {
            public string PersonName;
            public SmartJoinedClass6[] FavouriteClasses;
        }

        class SmartJoinedClass6
        {
            public string ClassName;
            public string TagName;
        }

        [Test]
        public async Task JoinInMap_With2LevelJoin_WithSingularProperty()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(q => q.TheTags.One().Name == Data.Tags.Sport.Name)
                .Map(query => new SmartJoinedClass5
                { 
                    PersonName = query.ThePerson.Name,
                    FavouriteClasses = query.TheClasses
                        .Select(c => new SmartJoinedClass6
                        {
                            ClassName = c.Name,
                            TagName = query.TheTags
                                .One()
                                .Name
                        })
                        .ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            var john = data.ElementAt(0);
            var mary = data.ElementAt(1);

            // John
            Assert.AreEqual(Data.People.John.Name, john.PersonName);
            
            Assert.AreEqual(2, john.FavouriteClasses.Length);
            Assert.AreEqual(Data.Classes.Tennis.Name, john.FavouriteClasses[0].ClassName);
            Assert.AreEqual(Data.Tags.Sport.Name, john.FavouriteClasses[0].TagName);
            Assert.AreEqual(Data.Classes.Archery.Name, john.FavouriteClasses[1].ClassName);
            Assert.AreEqual(Data.Tags.Sport.Name, john.FavouriteClasses[1].TagName);
            
            // Mary
            Assert.AreEqual(Data.People.Mary.Name, mary.PersonName);
            
            Assert.AreEqual(1, mary.FavouriteClasses.Length);
            Assert.AreEqual(Data.Classes.Tennis.Name, mary.FavouriteClasses[0].ClassName);
            Assert.AreEqual(Data.Tags.Sport.Name, mary.FavouriteClasses[0].TagName);
        }
    }
}
