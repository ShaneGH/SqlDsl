using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.Joins
{
    [SqlTestAttribute(SqlType.Sqlite)]
    public class MappedMultiJoinedTests : FullPathTestBase
    {
        public MappedMultiJoinedTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        class JoinedQueryClass : QueryContainer
        {
            public List<Purchase> ThePurchasesByClass { get; set; }
        }

        Dsl.IQuery<JoinedQueryClass> FullyJoinedQuery()
        {
            return SqlDsl.UnitTests.TestUtils.FullyJoinedQuery<JoinedQueryClass>(SqlType)
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
    }
}
