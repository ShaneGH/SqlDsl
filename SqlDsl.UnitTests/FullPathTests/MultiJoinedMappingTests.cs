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
            public List<PersonClass> PersonClasses { get; set; }
            public List<Class> Classes { get; set; }
            public List<ClassTag> ClassTags { get; set; }
            public List<Tag> Tags { get; set; }
            public List<Purchase> PurchasesByMe { get; set; }
            public List<Purchase> PurchasesByMeForMyClasses { get; set; }
        }

        static Dsl.IQuery<JoinedQueryClass> FullyJoinedQuery()
        {
            return Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .LeftJoin<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .LeftJoin<Class>(q => q.Classes)
                    .On((q, c) => q.PersonClasses.One().ClassId == c.Id)
                .LeftJoin<ClassTag>(q => q.ClassTags)
                    .On((q, ct) => q.Classes.One().Id == ct.ClassId)
                .LeftJoin<Tag>(q => q.Tags)
                    .On((q, t) => q.ClassTags.One().TagId == t.Id)
                .LeftJoin<Purchase>(q => q.PurchasesByMe)
                    .On((q, t) => q.ThePerson.Id == t.PersonId)
                .LeftJoin<Purchase>(q => q.PurchasesByMeForMyClasses)
                    .On((q, t) => q.ThePerson.Id == t.PersonId && q.Classes.One().Id == t.ClassId);
        }

        [Test]
        [Ignore("TODO")]
        public async Task SimpleMapOn1Table()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                // .Map(p => new SimpleMapClass
                // { 
                //     TheName = p.Name,
                //     Inner = new SimpleMapClass
                //     {
                //         TheName = p.Name
                //     }
                // })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            // Assert.AreEqual(2, data.Count());
            // Assert.AreEqual(Data.People.John.Name, data.First().TheName);
            // Assert.AreEqual(Data.People.John.Name, data.First().Inner.TheName);
            // Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1).TheName);
            // Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1).Inner.TheName);
        }
    }
}
