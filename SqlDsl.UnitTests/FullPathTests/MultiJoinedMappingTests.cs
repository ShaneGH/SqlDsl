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
            public List<Purchase> ThePurchasesByClass { get; set; }
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
                .LeftJoin<Purchase>(q => q.ThePurchasesByClass)
                    .On((q, t) => q.ThePerson.Id == t.PersonId && q.ThePerson.Id == t.PurchaedForPersonId && q.TheClasses.One().Id == t.ClassId);
        }

        [Test]
        [Ignore("TODO")]
        public async Task SimpleMapOn1Table_WithOneResult()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(p => new
                { 
                    Person = p.ThePerson.Name,
                    Classes = p.TheClasses
                        .Select(c => new
                        {
                            Name = c.Name,
                            Price = p.ThePurchasesByClass.One().Amount
                        })
                        .ToArray()
                })
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            
            Assert.AreEqual(Data.People.John.Name, data[0].Person);
            CollectionAssert.AreEqual(new [] 
            { 
                new { Name = Data.Classes.Tennis.Name, Price = Data.Purchases.JohnPurchasedHimselfTennis.Amount } 
            }, data[0].Classes);
        }

        [Test]
        [Ignore("TODO")]
        public async Task SimpleMapOn1Table_WithMultipleResults()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.Mary.Id)
                .Map(p => new
                { 
                    Person = p.ThePerson.Name,
                    Classes = p.TheClasses
                        .Select(c => new
                        {
                            Name = c.Name,
                            Price = p.ThePurchasesByClass.Select(x => x.Amount).ToArray()
                        })
                        .ToArray()
                })
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            
            Assert.AreEqual(Data.People.Mary.Name, data[0].Person);
            Assert.AreEqual(1, data[0].Classes.Length);
            Assert.AreEqual(Data.Classes.Tennis.Name, data[0].Classes[0].Name);
            Assert.AreEqual(Data.Purchases.MaryPurchasedHerselfTennis1, data[0].Classes[0].Price[0]);
            Assert.AreEqual(Data.Purchases.MaryPurchasedHerselfTennis2, data[0].Classes[0].Price[1]);
        }
    }
}
