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

        static void AssertMapOnTableWithJoinedTable(IEnumerable<JoinedMapClass> data)
        {
            //Assert.AreEqual(2, data.Count());
            Assert.AreEqual(1, data.Count());

            Assert.AreEqual(Data.People.John.Name, data.First().TheName);
            
            Assert.AreEqual(2, data.First().TheClassNames.Count());
            Assert.AreEqual(Data.Classes.Tennis.Name, data.First().TheClassNames.ElementAt(0));
            Assert.AreEqual(Data.Classes.Archery.Name, data.First().TheClassNames.ElementAt(1));
            Assert.AreEqual(2, data.First().TheClassNamesList.Count());
            Assert.AreEqual(Data.Classes.Tennis.Name, data.First().TheClassNamesList.ElementAt(0));
            Assert.AreEqual(Data.Classes.Archery.Name, data.First().TheClassNamesList.ElementAt(1));
            Assert.AreEqual(2, data.First().TheClassNamesArray.Count());
            Assert.AreEqual(Data.Classes.Tennis.Name, data.First().TheClassNamesArray.ElementAt(0));
            Assert.AreEqual(Data.Classes.Archery.Name, data.First().TheClassNamesArray.ElementAt(1));

            Assert.AreEqual(3, data.First().TheTagNames.Count());
            Assert.AreEqual(Data.Tags.Sport.Name, data.First().TheTagNames.ElementAt(0));
            Assert.AreEqual(Data.Tags.Sport.Name, data.First().TheTagNames.ElementAt(1));
            Assert.AreEqual(Data.Tags.BallSport.Name, data.First().TheTagNames.ElementAt(2));
            
            // Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1).TheName);

            // Assert.AreEqual(1, data.ElementAt(1).TheClassNames.Count());
            // Assert.AreEqual(Data.Classes.Tennis.Name, data.ElementAt(1).TheClassNames.ElementAt(0));
            // Assert.AreEqual(1, data.ElementAt(1).TheClassNamesList.Count());
            // Assert.AreEqual(Data.Classes.Tennis.Name, data.ElementAt(1).TheClassNamesList.ElementAt(0));
            // Assert.AreEqual(1, data.ElementAt(1).TheClassNamesArray.Count());
            // Assert.AreEqual(Data.Classes.Tennis.Name, data.ElementAt(1).TheClassNamesArray.ElementAt(0));

            // Assert.AreEqual(3, data.ElementAt(1).TheTagNames.Count());
            // Assert.AreEqual(Data.Tags.Sport.Name, data.ElementAt(1).TheTagNames.ElementAt(0));
        }

        [Test]
        public async Task MapOnTableWithJoinedTable()
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
                .Where(q => q.Person.Id == 1)
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
            AssertMapOnTableWithJoinedTable(data);
        }

        class JoinedQueryClass2
        {
            public JoinedQueryClass Query { get; set; }
        }

        [Test]
        [Ignore("TODO: this case")]
        public async Task MapOnTableWithJoinedTable_DataIsOnInnerProperty()
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
            AssertMapOnTableWithJoinedTable(data);
        }
    }
}
