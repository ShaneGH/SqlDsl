using NUnit.Framework;
using SqlDsl.Schema;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class SchemaAttributeTests : FullPathTestBase
    {
    
        [Table("Person")]
        public class PersonWithAttributes
        {
            public long Id { get; set; }
            public string Name { get; set; }
        }

        [Test]
        public void ClassWithTableName_GetsCorrectData()
        {
            // arrange
            // act
            var john = Sql.Query
                .Sqlite<PersonWithAttributes>()
                .Where(p => p.Id == Data.People.John.Id)
                .First(Executor, logger: Logger);

            // assert
            Assert.AreEqual(Data.People.John.Name, john.Name);
        }
    }
}