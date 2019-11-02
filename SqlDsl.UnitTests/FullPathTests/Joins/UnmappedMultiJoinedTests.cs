using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.Joins
{
    // [SqlTestAttribute(SqlType.TSql)]
    // [SqlTestAttribute(SqlType.MySql)]
    // [SqlTestAttribute(SqlType.Sqlite)]
    [TestFixture]
    public class UnmappedMultiJoinedTests : FullPathTestBase
    {
        public UnmappedMultiJoinedTests()
            : base(SqlType.Sqlite)
        {
        }
        // public UnmappedMultiJoinedTests(SqlType testFlavour)
        //     : base(testFlavour)
        // {
        // }
        
        class JoinedQueryClass : QueryContainer
        {
            public List<Purchase> ThePurchasesByClass { get; set; }
        }

        Dsl.IQuery<JoinedQueryClass> FullyJoinedQuery()
        {
            return SqlDsl.UnitTests.TestUtils
                .FullyJoinedQuery<JoinedQueryClass>(SqlType)
                .LeftJoinMany<Purchase>(q => q.ThePurchasesByClass)
                    .On((q, p) => q.ThePerson.Id == p.PersonId 
                        && q.ThePerson.Id == p.PurchaedForPersonId 
                        && q.TheClasses.One().Id == p.ClassId);
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
            CollectionAssert.AreEquivalent(
                Data.Tags.Where(t => t.Id != Data.Tags.UnusedTag.Id), 
                data[0].TheTags);
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
            CollectionAssert.AreEquivalent(
                Data.Tags.Where(t => t.Id != Data.Tags.UnusedTag.Id), 
                data[0].TheTags);
        }
    }
}
