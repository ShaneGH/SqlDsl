using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(TestFlavour.Sqlite)]
    public class QueryWithArgsTests : FullPathTestBase
    {
        public QueryWithArgsTests(TestFlavour testFlavour)
            : base(testFlavour)
        {
        }
        
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public IEnumerable<PersonClass> ThePersonClasses { get; set; }
            public IEnumerable<Class> TheClasses { get; set; }
            public IEnumerable<ClassTag> TheClassTags { get; set; }
            public IEnumerable<Tag> TheTags { get; set; }
        }

        public class Arguments
        {
            public long AValue;
        }

        [Test]
        public async Task QueryWithArgs_WithArgsInWhere_ExecutesCorrectly()
        {
            // arrange
            var query = Sql.Query
                .Sqlite<Arguments, JoinedQueryClass>()
                .From(x => x.ThePerson)
                .LeftJoin(x => x.ThePersonClasses)
                    .On((q, x) => x.PersonId == q.ThePerson.Id)
                .Where((x, args) => x.ThePersonClasses.One().ClassId == args.AValue)
                .Compile();

            // act
            var result = await query.ToIEnumerableAsync(Executor, new Arguments { AValue = Data.Classes.Tennis.Id });

            // assert
            Assert.AreEqual(2, result.Count());
            
            Assert.AreEqual(Data.People.John, result.First().ThePerson);
            Assert.AreEqual(1, result.First().ThePersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis, result.First().ThePersonClasses.First());

            Assert.AreEqual(Data.People.Mary, result.ElementAt(1).ThePerson);
            Assert.AreEqual(1, result.ElementAt(1).ThePersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, result.ElementAt(1).ThePersonClasses.First());
        }

        [Test]
        public async Task Select_JoinOnNonTable_ReturnsCorrectValues1()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<Arguments, JoinedQueryClass>()
                .From(result => result.ThePerson)
                .InnerJoin<PersonClass>(result => result.ThePersonClasses)
                    .On((q, c, a) => c.ClassId == a.AValue)
                .ToIEnumerableAsync(Executor, new Arguments { AValue = Data.Classes.Archery.Id });

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).ThePerson);

            foreach (var person in data)
            {
                Assert.AreEqual(1, person.ThePersonClasses.Count());
                Assert.AreEqual(Data.PersonClasses.JohnArchery, person.ThePersonClasses.First());
            }
        }

        [Test]
        public async Task Select_JoinOnNonTable_ReturnsCorrectValues2()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<Arguments, JoinedQueryClass>()
                .From(result => result.ThePerson)
                .InnerJoin<PersonClass>(result => result.ThePersonClasses)
                    .On((q, c, a) => c.ClassId == a.AValue)
                .ToIEnumerableAsync(Executor, new Arguments { AValue = Data.Classes.Tennis.Id });

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).ThePerson);

            foreach (var person in data)
            {
                Assert.AreEqual(2, person.ThePersonClasses.Count());
                Assert.AreEqual(Data.PersonClasses.JohnTennis, person.ThePersonClasses.First());
                Assert.AreEqual(Data.PersonClasses.MaryTennis, person.ThePersonClasses.ElementAt(1));
            }
        }

        [Test]
        public async Task Select_JoinTableAndNonTable_ReturnsCorrectValues()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<Arguments, JoinedQueryClass>()
                .From(result => result.ThePerson)
                .InnerJoin<PersonClass>(result => result.ThePersonClasses)
                    .On((q, c, a) => q.ThePerson.Id == c.PersonId && c.ClassId == a.AValue)
                .ToIEnumerableAsync(Executor, new Arguments { AValue = Data.Classes.Tennis.Id });

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).ThePerson);

            Assert.AreEqual(1, data.First().ThePersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis, data.First().ThePersonClasses.First());
            
            Assert.AreEqual(1, data.ElementAt(1).ThePersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.ElementAt(1).ThePersonClasses.First());
        }
    }
}
