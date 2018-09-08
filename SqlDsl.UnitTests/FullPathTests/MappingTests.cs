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
        class SimpleMapClass
        {
            public string TheName { get; set; }
            public SimpleMapClass Inner { get; set; }
            public IEnumerable<int> TheClassIds { get; set; }
            public IEnumerable<int> TheClassTagIds { get; set; }
        }

        [Test]
        public async Task SimpleMapOn1Table()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<Person>()
                .From()
                .Map(p => new SimpleMapClass
                { 
                    TheName = p.Name,
                    Inner = new SimpleMapClass
                    {
                        TheName = p.Name
                    }
                })
                .ExecuteAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Name, data.First().TheName);
            Assert.AreEqual(Data.People.John.Name, data.First().Inner.TheName);
            Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1).TheName);
            Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1).Inner.TheName);
        }

        [Test]
        public async Task MapOnTableWith1JoinedTable()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.Person)
                .InnerJoin<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.Person.Id == pc.PersonId)
                .Where(q => q.Person.Id == 1)
                .Map(p => new SimpleMapClass
                { 
                    TheName = p.Person.Name,
                    // TODO: new statement in select
                    TheClassIds = p.PersonClasses.Select(c => c.ClassId)
                })
                .ExecuteAsync(Executor);

            // assert
            Assert.AreEqual(1, data.Count());

            Assert.AreEqual(Data.People.John.Name, data.First().TheName);
            
            Assert.AreEqual(2, data.First().TheClassIds.Count());
            Assert.Contains(Data.Classes.Tennis.Id, data.First().TheClassIds.ToList());
            Assert.Contains(Data.Classes.Archery.Id, data.First().TheClassIds.ToList());
        }

        [Test]
        [Ignore("TODO: look at this case and throw better exception")]
        public async Task Exploritory()
        {
            // arrange
            // act
            await Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.Person)
                .InnerJoin<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.Person.Id == pc.PersonId)
                .InnerJoin<ClassTag>(q => q.ClassTags)
                    .On((q, ct) => q.Classes.One().Id == ct.ClassId)
                .Where(q => q.Person.Id == 1)
                .ExecuteAsync(Executor);
        }

        [Test]
        public async Task MapOnTableWith2JoinedTables()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.Person)
                .InnerJoin<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.Person.Id == pc.PersonId)
                .InnerJoin<ClassTag>(q => q.ClassTags)
                    .On((q, ct) => q.PersonClasses.One().ClassId == ct.ClassId)
                .Where(q => q.Person.Id == 1)
                .Map(p => new SimpleMapClass
                { 
                    TheName = p.Person.Name,
                    TheClassIds = p.PersonClasses.Select(c => c.ClassId),
                    TheClassTagIds = p.ClassTags.Select(c => c.TagId)
                })
                .ExecuteAsync(Executor);

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
                .From<Person>(x => x.Person)
                .InnerJoin<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.Person.Id == pc.PersonId)
                .Where(q => q.Person.Id == 1)
                .Map(p => new SimpleMapClass
                { 
                    TheClassIds = p.PersonClasses.Select(c => c.ClassId)
                })
                .ExecuteAsync(Executor);

            // assert
            Assert.AreEqual(1, data.Count());
            
            Assert.AreEqual(2, data.First().TheClassIds.Count());
            Assert.Contains(Data.Classes.Tennis.Id, data.First().TheClassIds.ToList());
            Assert.Contains(Data.Classes.Archery.Id, data.First().TheClassIds.ToList());
        }

        class JoinedQueryClass
        {
            public Person Person { get; set; }
            public List<PersonClass> PersonClasses { get; set; }
            public List<Class> Classes { get; set; }
            public List<ClassTag> ClassTags { get; set; }
            public List<Tag> Tags { get; set; }
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
                .From<Person>(x => x.Person)
                .InnerJoin<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.Person.Id == pc.PersonId)
                .InnerJoin<Class>(q => q.Classes)
                    .On((q, c) => q.PersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.ClassTags)
                    .On((q, ct) => q.Classes.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.Tags)
                    .On((q, t) => q.ClassTags.One().TagId == t.Id)
                .Map(p => new JoinedMapClass
                { 
                    TheName = p.Person.Name,
                    // TODO: new statement in select
                    TheClassNames = p.Classes.Select(c => c.Name),
                    TheClassNamesList = p.Classes.Select(c => c.Name).ToList(),
                    TheClassNamesArray = p.Classes.Select(c => c.Name).ToArray(),
                    TheTagNames = p.Tags.Select(t => t.Name)
                })
                .ExecuteAsync(Executor);

            // assert
            AssertMapOnTableWith2JoinedTables(data);
        }

        class JoinedQueryClass2
        {
            public JoinedQueryClass Query { get; set; }
        }

        [Test]
        public async Task MapOnTableWith2JoinedTables_DataIsOnInnerProperty()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<JoinedQueryClass2>()
                .From<Person>(x => x.Query.Person)
                .InnerJoin<PersonClass>(q => q.Query.PersonClasses)
                    .On((q, pc) => q.Query.Person.Id == pc.PersonId)
                .InnerJoin<Class>(q => q.Query.Classes)
                    .On((q, c) => q.Query.PersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.Query.ClassTags)
                    .On((q, ct) => q.Query.Classes.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.Query.Tags)
                    .On((q, t) => q.Query.ClassTags.One().TagId == t.Id)
                .Map(p => new JoinedMapClass
                { 
                    TheName = p.Query.Person.Name,
                    TheClassNames = p.Query.Classes.Select(c => c.Name),
                    TheClassNamesList = p.Query.Classes.Select(c => c.Name).ToList(),
                    TheClassNamesArray = p.Query.Classes.Select(c => c.Name).ToArray(),
                    TheTagNames = p.Query.Tags.Select(t => t.Name)
                })
                .ExecuteAsync(Executor);

            // assert
            AssertMapOnTableWith2JoinedTables(data);
        }
    }
}
