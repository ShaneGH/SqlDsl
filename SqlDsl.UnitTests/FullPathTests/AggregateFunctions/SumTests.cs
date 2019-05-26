using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.AggregateFunctions
{
    [SqlTestAttribute(SqlType.MySql)]
    [SqlTestAttribute(SqlType.Sqlite)]
    public class SumTests : FullPathTestBase
    {
        public SumTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        [Test]
        public async Task SumAndGroup_WithPureSum()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType, false)
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Select(c => c.Id + p.ThePersonClasses.One().ClassId).Sum()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 14L
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 6L
                }
            }, data);
        }

        [Test]
        public async Task SumAndGroup_WithMapperInSum()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType, false)
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Sum(c => c.Id + p.ThePersonClasses.One().ClassId)
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 14L
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 6L
                }
            }, data);
        }
    }
}
