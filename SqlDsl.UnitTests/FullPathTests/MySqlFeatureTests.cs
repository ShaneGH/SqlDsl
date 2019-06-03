using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(SqlType.MySql)]
    public class MySqlFeatureTests : FullPathTestBase
    {
        public MySqlFeatureTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        [Test]
        public void LegacyRowNumber_SmokeTest()
        {            
            // arrange
            // act
            // assert
            Sql.Query
                .MySql<QueryContainer>(settings: new MySqlSettings
                {
                    Version8OrHigher = false
                })
                .From(x => x.ThePerson)
                .InnerJoinMany<PersonClass>(x => x.ThePersonClasses)
                    .On((x, pc) => x.ThePerson.Id == pc.PersonId)
                .ToArray(Executor, logger: Logger);
        }
        
        [Test]
        public void V8RowNumber_SmokeTest()
        {
            // arrange
            // act
            // assert
            Sql.Query
                .MySql<QueryContainer>(settings: new MySqlSettings
                {
                    Version8OrHigher = true
                })
                .From(x => x.ThePerson)
                .InnerJoinMany<PersonClass>(x => x.ThePersonClasses)
                    .On((x, pc) => x.ThePerson.Id == pc.PersonId)
                .ToArray(Executor, logger: Logger);
        }
        
        [Test]
        [Ignore("TODO: re-enable and order correctly when a decision has been made on ROW_NUMBER() OVER ORDER BY")]
        public void LegacyRowNumber_OrderingIsConsistent()
        {
            // arrange
            // act
            var data = Sql.Query
                .MySql<ClassTag>(settings: new MySqlSettings
                {
                    Version8OrHigher = false
                })
                .OrderBy(t => t.ClassId)
                .ThenBy(t => t.TagId)
                .ToArray(Executor, logger: Logger);
                
                Assert.Fail();

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                Data.ClassTags.TennisSport,
                Data.ClassTags.TennisBallSport
            }, data);
        }
        
        [Test]
        [Ignore("TODO: re-enable and order correctly when a decision has been made on ROW_NUMBER() OVER ORDER BY")]
        public void V8RowNumber_OrderingIsConsistent()
        {
            // arrange
            // act
            var data = Sql.Query
                .MySql<ClassTag>(settings: new MySqlSettings
                {
                    Version8OrHigher = true
                })
                .OrderBy(t => t.ClassId)
                .ThenBy(t => t.TagId)
                .ToArray(Executor, logger: Logger);

                Assert.Fail();
                
            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                Data.ClassTags.TennisSport,
                Data.ClassTags.TennisBallSport
            }, data);
        }
    }
}
