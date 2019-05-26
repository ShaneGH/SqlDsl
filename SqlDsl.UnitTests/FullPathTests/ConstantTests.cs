using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(TestFlavour.Sqlite)]
    public class ConstantTests : FullPathTestBase
    {
        public ConstantTests(TestFlavour testFlavour)
            : base(testFlavour)
        {
        }
        

        [Test]
        public async Task MapAndReturnConstant()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => 77)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First());
            Assert.AreEqual(77, data.ElementAt(1));
        }

        class AnInt
        {
            public int IntValue;
        }

        [Test]
        public async Task MapAndReturnPieceOfConstant()
        {
            // arrange
            var anInt = new AnInt { IntValue = 77 };

            // act
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => anInt.IntValue)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First());
            Assert.AreEqual(77, data.ElementAt(1));
        }

        [Test]
        public void ConstantInJoin()
        {
            // arrange
            // act
            var data = Sql.Query.Sqlite<QueryContainer>()
                .From(x => x.ThePerson)
                .InnerJoin(q => q.ThePersonsData)
                    .On((q, pc) => q.ThePerson.Id == 77)
                .Map((p) => 7)
                .ToArray(Executor, logger: Logger);

            // assert
            Assert.AreEqual(0, data.Length);
        }

        [Test]
        public void ConstantInOrderBy()
        {
            // arrange
            // act
            // assert
            TestUtils
                .FullyJoinedQuery()
                .OrderBy(x => 77)
                .ToArray(Executor, logger: Logger);

            // no exception is good enough
        }

        [Test]
        public void ConstantInPaging()
        {
            // arrange
            // act
            // assert
            TestUtils
                .FullyJoinedQuery()
                .Skip(1).Take(1)
                .ToArray(Executor, logger: Logger);

            // no exception is good enough
        }
    }
}