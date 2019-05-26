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
                .InnerJoin<PersonClass>(x => x.ThePersonClasses)
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
                .InnerJoin<PersonClass>(x => x.ThePersonClasses)
                    .On((x, pc) => x.ThePerson.Id == pc.PersonId)
                .ToArray(Executor, logger: Logger);
        }
    }
}
