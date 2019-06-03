using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.Joins
{
    [SqlTestAttribute(SqlType.MySql)]
    [SqlTestAttribute(SqlType.Sqlite)]
    public class UnmappedJoinTests : FullPathTestBase
    {
        public UnmappedJoinTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        [Test]
        public async Task Select2Joins_DoesNotDuplicateRecords()
        {
            // arrange
            // act
            var data = await Query<JoinTests.QueryClass>()
                .From(result => result.ThePerson)
                .InnerJoinMany<PersonClass>(result => result.ThePersonClasses)
                    .On((q, c) => q.ThePerson.Id == c.PersonId)
                .InnerJoinMany<ClassTag>(result => result.TheClassTags)
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
        [Ignore("Does not work in MySql")]
        public async Task Select2Joins_Backwards_DoesNotDuplicateRecords()
        {
            // arrange
            // act
            var data = await Query<JoinTests.QueryClass>()
                .From(result => result.ThePerson)
                .InnerJoinMany<ClassTag>(result => result.TheClassTags)
                    .On((q, c) => q.ThePersonClasses.Single().ClassId == c.ClassId)
                .InnerJoinMany<PersonClass>(result => result.ThePersonClasses)
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