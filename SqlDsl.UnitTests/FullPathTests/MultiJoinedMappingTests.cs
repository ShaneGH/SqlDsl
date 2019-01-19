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
                            Price = (float?)p.ThePurchasesByClass.One().Amount
                        })
                        .ToArray()
                })
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            
            Assert.AreEqual(Data.People.John.Name, data[0].Person);
            CollectionAssert.AreEqual(new [] 
            { 
                new { Name = Data.Classes.Tennis.Name, Price = (float?)Data.Purchases.JohnPurchasedHimselfTennis.Amount },
                new { Name = Data.Classes.Archery.Name, Price = (float?)null } 
            }, data[0].Classes);
        }

        class SimpleMapOn1Table_WithMultipleResultsResult
        {
            public string ThePerson;
            public SimpleMapOn1Table_WithMultipleResultsClassResult[] TheClasses;
        }

        class SimpleMapOn1Table_WithMultipleResultsClassResult
        {
            public string Name;
            public float[] Prices;
        }

        [Test]
        public async Task SimpleMapOn1Table_WithMultipleResults_AndProperties()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.Mary.Id)
                .Map(p => new SimpleMapOn1Table_WithMultipleResultsResult
                { 
                    ThePerson = p.ThePerson.Name,
                    TheClasses = p.TheClasses
                        .Select(c => new SimpleMapOn1Table_WithMultipleResultsClassResult
                        {
                            Name = c.Name,
                            Prices = p.ThePurchasesByClass.Select(x => x.Amount).ToArray()
                        })
                        .ToArray()
                })
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            
            Assert.AreEqual(Data.People.Mary.Name, data[0].ThePerson);
            Assert.AreEqual(1, data[0].TheClasses.Length);
            Assert.AreEqual(Data.Classes.Tennis.Name, data[0].TheClasses[0].Name);
            Assert.AreEqual(2, data[0].TheClasses[0].Prices.Length);
            Assert.AreEqual(Data.Purchases.MaryPurchasedHerselfTennis1.Amount, data[0].TheClasses[0].Prices[0]);
            Assert.AreEqual(Data.Purchases.MaryPurchasedHerselfTennis2.Amount, data[0].TheClasses[0].Prices[1]);
        }

        [Test]
        public async Task SimpleMapOn1Table_WithMultipleResults_AndconstructorArgs()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.Mary.Id)
                .Map(p => new
                { 
                    ThePerson = p.ThePerson.Name,
                    TheClasses = p.TheClasses
                        .Select(c => new
                        {
                            Name = c.Name,
                            Prices = p.ThePurchasesByClass.Select(x => x.Amount).ToArray()
                        })
                        .ToArray()
                })
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            
            Assert.AreEqual(Data.People.Mary.Name, data[0].ThePerson);
            Assert.AreEqual(1, data[0].TheClasses.Length);
            Assert.AreEqual(Data.Classes.Tennis.Name, data[0].TheClasses[0].Name);
            Assert.AreEqual(2, data[0].TheClasses[0].Prices.Length);
            Assert.AreEqual(Data.Purchases.MaryPurchasedHerselfTennis1.Amount, data[0].TheClasses[0].Prices[0]);
            Assert.AreEqual(Data.Purchases.MaryPurchasedHerselfTennis2.Amount, data[0].TheClasses[0].Prices[1]);
        }

        [Test]
        public async Task SimpleNonMapOn1Table_WithOneResult()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            CollectionAssert.AreEquivalent(Data.ClassTags, data[0].TheClassTags);
            CollectionAssert.AreEquivalent(Data.Classes, data[0].TheClasses);
            Assert.AreEqual(Data.People.John, data[0].ThePerson);
            CollectionAssert.AreEquivalent(
                Data.PersonClasses.Where(p => p.PersonId == Data.People.John.Id), 
                data[0].ThePersonClasses);
            CollectionAssert.AreEquivalent(
                new []{ Data.Purchases.JohnPurchasedHimselfTennis }, 
                data[0].ThePurchasesByClass);
            CollectionAssert.AreEquivalent(Data.Tags, data[0].TheTags);
        }

        [Test]
        public async Task SimpleNonMapOn1Table_WithMultipleResults_AndconstructorArgs()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.Mary.Id)
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            CollectionAssert.AreEquivalent(
                Data.ClassTags.Where(t => t.ClassId == Data.Classes.Tennis.Id), 
                data[0].TheClassTags);
            CollectionAssert.AreEquivalent(
                Data.Classes.Where(t => t.Id == Data.Classes.Tennis.Id), 
                data[0].TheClasses);
            Assert.AreEqual(Data.People.Mary, data[0].ThePerson);
            CollectionAssert.AreEquivalent(
                Data.PersonClasses.Where(p => p.PersonId == Data.People.Mary.Id), 
                data[0].ThePersonClasses);
            CollectionAssert.AreEquivalent(
                new []{ Data.Purchases.MaryPurchasedHerselfTennis1, Data.Purchases.MaryPurchasedHerselfTennis2 }, 
                data[0].ThePurchasesByClass);
            CollectionAssert.AreEquivalent(Data.Tags, data[0].TheTags);
        }
    }
}
