using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.AggregateFunctions
{
    [SqlTestAttribute(SqlType.MySql)]
    [SqlTestAttribute(SqlType.Sqlite)]
    public class MinTests : FullPathTestBase
    {
        public MinTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        [Test]
        public async Task MinAndGroup_WithPureMin()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Select(c => c.Id + p.ThePersonClasses.One().ClassId).Min()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 6L
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 6L
                }
            }, data);
        }

        [Test]
        public async Task MinAndGroup_WithMapperInMin()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Min(c => c.Id + p.ThePersonClasses.One().ClassId)
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 6L
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
