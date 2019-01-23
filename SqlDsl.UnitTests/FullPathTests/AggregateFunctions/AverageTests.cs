using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.AggregateFunctions
{
    [TestFixture]
    public class AverageTests : FullPathTestBase
    {
        [Test]
        public async Task AverageAndGroup_WithPureAverage()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(false)
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Select(c => c.Id + p.ThePersonClasses.One().ClassId).Average()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 7D
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 6D
                }
            }, data);
        }

        [Test]
        public async Task AverageAndGroup_WithMapperInAverage()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(false)
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Average(c => c.Id + p.ThePersonClasses.One().ClassId)
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 7D
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 6D
                }
            }, data);
        }
    }
}
