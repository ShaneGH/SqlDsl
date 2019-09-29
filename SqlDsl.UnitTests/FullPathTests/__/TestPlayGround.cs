using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using SqlDsl.DataParser;
using SqlDsl.Mapper;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class TestPlayground : FullPathTestBase
    {
        public TestPlayground()
            : base(SqlType.Sqlite)
        {
        }

        class MappedClass
        {
            public string ClassName;
            public IEnumerable<string> TagNames;
        }

        class MappedPerson 
        {
            public string PersonName;
            public IEnumerable<MappedClass> MappedClasses;
        }

        [Test]
        public void TestTestTest()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == 1)
                .OrderBy(x => x.ThePersonsData.PersonId + x.ThePerson.Id)
                .Map(x => new MappedPerson
                {
               //     PersonName = x.ThePerson.Name,
                    MappedClasses = x.TheClasses
                        .Select(c => new MappedClass
                        {
                  //          ClassName = c.Name,
                            TagNames = x.TheTags
                                .Select(t => t.Name)
                                .ToList()


                        })
                })
                .ToList(Executor);

            // assert
            // Assert.AreEqual(1, data.Count);
            // Assert.AreEqual(Data.People.John.Name, data[0].PersonName);
        }
    }
}
