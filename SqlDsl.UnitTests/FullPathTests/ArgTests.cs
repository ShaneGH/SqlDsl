using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(TestFlavour.Sqlite)]
    public class ArgTests : FullPathTestBase
    {
        public ArgTests(TestFlavour testFlavour)
            : base(testFlavour)
        {
        }

        [Test]
        public async Task MapAndReturnArg()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQueryWithArg<int>(TestFlavour)
                .Map((p, a) => a)
                .ToIEnumerableAsync(Executor, 77, logger: Logger);

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
        public async Task MapAndReturnPieceOfArg()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQueryWithArg<AnInt>(TestFlavour)
                .Map((p, a) => a.IntValue)
                .ToIEnumerableAsync(Executor, new AnInt { IntValue = 77 }, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First());
            Assert.AreEqual(77, data.ElementAt(1));
        }

        [Test]
        public void ArgInJoin()
        {
            // arrange
            // act
            var data = Query<int, QueryContainer>()
                .From(x => x.ThePerson)
                .InnerJoin(q => q.ThePersonsData)
                    .On((q, pc, a) => q.ThePerson.Id == a)
                .Map((p, a) => 7)
                .ToArray(Executor, 77, logger: Logger);

            // assert
            Assert.AreEqual(0, data.Length);
        }

        [Test]
        public void ArgInOrderBy()
        {
            // arrange
            // act
            // assert
            TestUtils
                .FullyJoinedQueryWithArg<int>(TestFlavour)
                .OrderBy((x, a) => a)
                .ToArray(Executor, 77, logger: Logger);

            // no exception is good enough
        }

        [Test]
        public void ArgInPaging()
        {
            // arrange
            // act
            // assert
            TestUtils
                .FullyJoinedQueryWithArg<int>(TestFlavour)
                .Skip(a => a).Take(a => a)
                .ToArray(Executor, 1, logger: Logger);

            // no exception is good enough
        }
    }
}