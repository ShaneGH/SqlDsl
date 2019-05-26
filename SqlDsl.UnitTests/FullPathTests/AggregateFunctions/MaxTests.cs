using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.AggregateFunctions
{
    [SqlTestAttribute(SqlType.MySql)]
    public class MaxTests : FullPathTestBase
    {
        public MaxTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        [Test]
        public async Task MaxAndGroup_WithPureMax()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Select(c => c.Id + p.ThePersonClasses.One().ClassId).Max()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 8L
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 6L
                }
            }, data);
        }

        [Test]
        public async Task MaxAndGroup_WithMapperInMax()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Max(c => c.Id + p.ThePersonClasses.One().ClassId)
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 8L
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
