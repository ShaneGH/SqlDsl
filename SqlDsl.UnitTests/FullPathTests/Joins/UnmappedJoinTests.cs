using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.Joins
{
    [TestFixture]
    public class UnmappedJoinTests : FullPathTestBase
    {
        [Test]
        public async Task Select2Joins_DoesNotDuplicateRecords()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<JoinTests.QueryClass>()
                .From(result => result.ThePerson)
                .InnerJoin<PersonClass>(result => result.ThePersonClasses)
                    .On((q, c) => q.ThePerson.Id == c.PersonId)
                .InnerJoin<ClassTag>(result => result.TheClassTags)
                    .On((q, c) => q.ThePersonClasses.First().ClassId == c.ClassId)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            JoinTests.AssertSelect1SimpleJoin(data);
            
            Assert.AreEqual(3, data.First().TheClassTags.Count());
            Assert.AreEqual(Data.ClassTags.TennisSport, data.First().TheClassTags.ElementAt(0));
            Assert.AreEqual(Data.ClassTags.TennisBallSport, data.First().TheClassTags.ElementAt(1));
            Assert.AreEqual(Data.ClassTags.ArcherySport, data.First().TheClassTags.ElementAt(2));

            Assert.AreEqual(2, data.ElementAt(1).TheClassTags.Count());
            Assert.AreEqual(Data.ClassTags.TennisSport, data.First().TheClassTags.ElementAt(0));
            Assert.AreEqual(Data.ClassTags.TennisBallSport, data.First().TheClassTags.ElementAt(1));
        }

        [Test]
        public async Task Select2Joins_Backwards_DoesNotDuplicateRecords()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<JoinTests.QueryClass>()
                .From(result => result.ThePerson)
                .InnerJoin<ClassTag>(result => result.TheClassTags)
                    .On((q, c) => q.ThePersonClasses.Single().ClassId == c.ClassId)
                .InnerJoin<PersonClass>(result => result.ThePersonClasses)
                    .On((q, c) => q.ThePerson.Id == c.PersonId)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            JoinTests.AssertSelect1SimpleJoin(data);
            
            Assert.AreEqual(3, data.First().TheClassTags.Count());
            Assert.AreEqual(Data.ClassTags.TennisSport, data.First().TheClassTags.ElementAt(0));
            Assert.AreEqual(Data.ClassTags.TennisBallSport, data.First().TheClassTags.ElementAt(1));
            Assert.AreEqual(Data.ClassTags.ArcherySport, data.First().TheClassTags.ElementAt(2));

            Assert.AreEqual(2, data.ElementAt(1).TheClassTags.Count());
            Assert.AreEqual(Data.ClassTags.TennisSport, data.First().TheClassTags.ElementAt(0));
            Assert.AreEqual(Data.ClassTags.TennisBallSport, data.First().TheClassTags.ElementAt(1));
        }
    }
}