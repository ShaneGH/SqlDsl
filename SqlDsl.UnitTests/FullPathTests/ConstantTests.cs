using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(SqlType.TSql)]
    [SqlTestAttribute(SqlType.MySql)]
    [SqlTestAttribute(SqlType.Sqlite)]
    public class ConstantTests : FullPathTestBase
    {
        public ConstantTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        

        [Test]
        public async Task MapAndReturnConstant()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
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
                .FullyJoinedQuery(SqlType)
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
            var data = Query<QueryContainer>()
                .From(x => x.ThePerson)
                .InnerJoinOne(q => q.ThePersonsData)
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
                .FullyJoinedQuery(SqlType)
                .OrderBy(x => x.ThePerson.Id + 77)
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
                .FullyJoinedQuery(SqlType)
                .Skip(1).Take(1)
                .ToArray(Executor, logger: Logger);

            // no exception is good enough
        }
    }
}