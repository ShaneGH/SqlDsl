using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class SqlCaseTests : FullPathTestBase
    {
        [Test]
        public void SqlCase_ReturnsCorrectResult()
        {
            // arrange
            // act
            var result = Sql.Query
                .Sqlite<Tag>()
                .Map(t => new
                {
                    n = t.Name,
                    cs = Sql.Case
                        .When(t.Name == Data.Tags.BallSport.Name)
                        .Then(1)
                        .When(t.Name == Data.Tags.Sport.Name)
                        .Then(10)
                        .Else(100)
                })
                .ToArray(executor: Executor, logger: Logger);

            // assert
            CollectionAssert.AreEquivalent(new [] 
            { 
                new { n = Data.Tags.BallSport.Name, cs = 1 }, 
                new { n = Data.Tags.Sport.Name, cs = 10 }, 
                new { n = Data.Tags.UnusedTag.Name, cs = 100 }
            }, result);
        }
        
        [Test]
        public void SqlSimpleCase_ReturnsCorrectResult()
        {
            // arrange
            // act
            var result = Sql.Query
                .Sqlite<Tag>()
                .Map(t => new
                {
                    n = t.Name,
                    cs = Sql.Case
                        .Simple(t.Name)
                        .When(Data.Tags.BallSport.Name)
                        .Then(1)
                        .When(Data.Tags.Sport.Name)
                        .Then(10)
                        .Else(100)
                })
                .ToArray(executor: Executor, logger: Logger);

            // assert
            CollectionAssert.AreEquivalent(new [] 
            { 
                new { n = Data.Tags.BallSport.Name, cs = 1 }, 
                new { n = Data.Tags.Sport.Name, cs = 10 }, 
                new { n = Data.Tags.UnusedTag.Name, cs = 100 }
            }, result);
        }
    }
}