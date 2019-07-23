using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using SqlDsl;
using SqlDsl.Mapper;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(SqlType.MySql)]
    [SqlTestAttribute(SqlType.Sqlite)]
    public class ParallelObjectsTests : FullPathTestBase
    {
        public ParallelObjectsTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }

        private class PersonWithStuff
        {
        }
        
        [Test]
        public void GetObjectWithToUnrelatedProperties()
        {
            // arrange
            // act
            var data = TestUtils
                .Query<(Person Person, IEnumerable<PersonClass> pc, IEnumerable<Class> cls, IEnumerable<Purchase> ps)>(SqlType)
                .From(x => x.Person)
                .InnerJoinMany(x => x.pc).On((x, y) => x.Person.Id == y.PersonId)
                .InnerJoinMany(x => x.cls).On((x, y) => x.pc.One().ClassId == y.Id)
                .InnerJoinMany(x => x.ps).On((x, y) => x.Person.Id == y.PersonId)
                .Where(q => q.Person.Id == Data.People.John.Id)
                .Map(p => new
                {
                    name = p.Person.Name,
                    classes = p.cls.Select(c => c.Name),
                    purchases = p.ps.Select(ps => ps.Amount)
                })
                .First(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEquivalent(new [] { Data.Classes.Archery.Name, Data.Classes.Tennis.Name }, data.classes);
            CollectionAssert.AreEquivalent(new [] 
            { 
                Data.Purchases.JohnPurchasedHimselfShoes.Amount, 
                Data.Purchases.JohnPurchasedHimselfTennis.Amount
            }, data.purchases);
        }
    }
}
