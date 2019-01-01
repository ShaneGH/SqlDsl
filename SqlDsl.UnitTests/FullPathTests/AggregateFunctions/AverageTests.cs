using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.AggregateFunctions
{
    [TestFixture]
    public class AverageTests : FullPathTestBase
    {
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public List<PersonClass> ThePersonClasses { get; set; }
            public List<Class> TheClasses { get; set; }
            public List<ClassTag> TheClassTags { get; set; }
            public List<Tag> TheTags { get; set; }
        }

        static Dsl.IQuery<TArg, JoinedQueryClass> FullyJoinedQuery<TArg>()
        {
            return Sql.Query.Sqlite<TArg, JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id);
        }

        [Test]
        public async Task AverageAndGroup_WithPureAverage()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Select(c => c.Id + p.ThePersonClasses.One().ClassId).Average()
                })
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 7D
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 6D
                }
            }, data);
        }

        [Test]
        public async Task AverageAndGroup_WithMapperInAverage()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Average(c => c.Id + p.ThePersonClasses.One().ClassId)
                })
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 7D
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 6D
                }
            }, data);
        }
    }
}
