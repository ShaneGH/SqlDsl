using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(SqlType.MySql)]
    public class RemoveValuesFromInnerQueryTests : FullPathTestBase
    {
        public RemoveValuesFromInnerQueryTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        [Test]
        public async Task OrderByTableNotInMap()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType, false)
                .OrderBy(x => x.TheTags.One().Id)
                .Map(q => q.ThePerson.Name)
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.John.Name, Data.People.Mary.Name }, data);
        }

        [Test]
        public async Task WhereTableNotInMap()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType, false)
                .Where(x => x.TheClasses.One().Id == Data.Classes.Archery.Id)
                .Map(q => q.ThePerson.Name)
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.John.Name }, data);
        }

        [Test]
        public void RemovesUnusedTablesForNonStrictJoin()
        {
            // arrange
            // act
            var data = Query<QueryContainer>(false)
                .From(x => x.ThePerson)
                .InnerJoin(q => q.ThePersonsData)
                    .On((q, pc) => q.ThePerson.Id == 77)
                .Map(p => p.ThePerson.Name)
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEquivalent(new[]{ Data.People.John.Name, Data.People.Mary.Name  }, data);
        }

        [Test]
        public void RetainsUnusedTablesForStrictJoin()
        {
            // arrange
            // act
            var data = Query<QueryContainer>()
                .From(x => x.ThePerson)
                .InnerJoin(q => q.ThePersonsData)
                    .On((q, pc) => q.ThePerson.Id == 77)
                .Map(p => p.ThePerson.Name)
                .ToArray(Executor, logger: Logger);

            // assert
            Assert.AreEqual(0, data.Length);
        }
    }
}
