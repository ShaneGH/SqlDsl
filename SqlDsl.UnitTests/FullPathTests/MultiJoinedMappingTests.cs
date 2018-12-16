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
    public class MultiJoinedMappingTests : FullPathTestBase
    {
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public List<PersonClass> ThePersonClasses { get; set; }
            public List<Class> TheClasses { get; set; }
            public List<ClassTag> TheClassTags { get; set; }
            public List<Tag> TheTags { get; set; }
            public List<Purchase> ThePurchasesByMe { get; set; }
            public List<Purchase> ThePurchasesByMeForMyClasses { get; set; }
        }

        static Dsl.IQuery<JoinedQueryClass> FullyJoinedQuery()
        {
            return Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .LeftJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .LeftJoin<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .LeftJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .LeftJoin<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id)
                .LeftJoin<Purchase>(q => q.ThePurchasesByMe)
                    .On((q, t) => q.ThePerson.Id == t.PersonId);
                // .LeftJoin<Purchase>(q => q.PurchasesByMeForMyClasses)
                //     .On((q, t) => q.ThePerson.Id == t.PersonId && q.Classes.One().Id == t.ClassId);
        }

        [Test]
        public async Task SimpleMapOn1Table()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Map(p => new
                { 
                    TheName = p.ThePerson.Name,
                    Inner = new
                    {
                        TheName = p.ThePerson.Name
                    }
                })
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Name, data[0].TheName);
            Assert.AreEqual(Data.People.John.Name, data[0].Inner.TheName);
            Assert.AreEqual(Data.People.Mary.Name, data[1].TheName);
            Assert.AreEqual(Data.People.Mary.Name, data[1].Inner.TheName);
        }
    }
}
