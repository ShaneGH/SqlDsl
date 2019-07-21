using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using SqlDsl.Schema;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class CompositeKeyTests : FullPathTestBase
    {
        public CompositeKeyTests()
            : base(SqlType.MySql)
        {
        }
    
        [Table("PersonClass")]
        public class PersonClassWithAttributes : EqComparer
        {
            [Key]
            public long PersonId { get; set; }

            [Key]
            public long ClassId { get; set; }

            public override int GetHashCode() => $"{PersonId}.{ClassId}".GetHashCode();

            public override bool Equals(object p)
            {
                return Equals(p as PersonClass) || Equals(p as PersonClassWithAttributes);
            }

            public bool Equals(PersonClassWithAttributes person)
            {
                return person != null && person.PersonId == PersonId && person.ClassId == ClassId;
            }
            
            public bool Equals(PersonClass person)
            {
                return person != null && person.PersonId == PersonId && person.ClassId == ClassId;
            }
        }

        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public List<PersonClassWithAttributes> ThePersonClasses { get; set; }
        }

        Dsl.IQuery<JoinedQueryClass> FullyJoinedQuery()
        {
            return Query<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoinMany<PersonClassWithAttributes>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId);
        }

        [Test]
        public void ClassWithCompositeKey_GetsCorrectData()
        {
            // arrange
            // act
            var results = Query<PersonClassWithAttributes>()
                .ToList(Executor, logger: Logger);
                
            // assert
            Assert.AreEqual(3, results.Count);

            // NOTE: actual/expected are reversed here for equivelancy reasons
            CollectionAssert.AreEquivalent(results, new [] 
            {
                Data.PersonClasses.JohnArchery,
                Data.PersonClasses.JohnTennis,
                Data.PersonClasses.MaryTennis
            });
        }

        class Q
        {
            // warning CS0649: Field 'CompositeKeyTests.Q.Pc1/2' is never assigned to, and will always have its default value null
            #pragma warning disable 0649
            public PersonClassWithAttributes Pc1;
            public IEnumerable<PersonClassWithAttributes> Pc2;
            #pragma warning restore 0649
        }

        [Test]
        public void ClassWithCompositeKeyAndJoin_GetsCorrectData()
        {
            // arrange
            // act
            var results = Query<Q>()
                .From(x => x.Pc1)
                .InnerJoinMany(x => x.Pc2).On((q, x) => q.Pc1.PersonId == x.PersonId)
                .ToList(Executor, logger: Logger);
                
            // assert
            // NOTE: actual/expected are reversed here for equivelancy reasons
            CollectionAssert.AreEquivalent(
                results.Select(x => x.Pc1), 
                new [] 
                {
                    Data.PersonClasses.JohnArchery,
                    Data.PersonClasses.JohnTennis,
                    Data.PersonClasses.MaryTennis
                });

            foreach (var r in results)
            {
                CollectionAssert.AreEquivalent(
                    r.Pc2,
                    Data.PersonClasses.Where(p => p.PersonId == r.Pc1.PersonId));
            }
        }

        [Test]
        public void ClassWithCompositeKeyAndJoin_AndMapping_GetsCorrectData()
        {
            // arrange
            // act
            var results = Query<Q>()
                .From(x => x.Pc1)
                .InnerJoinMany(x => x.Pc2).On((q, x) => q.Pc1.PersonId == x.PersonId)
                .Map(x => new
                {
                    Pc1 = x.Pc1,
                    Pc2 = x.Pc2
                })
                .ToList(Executor, logger: Logger);
                
            // assert
            // NOTE: actual/expected are reversed here for equivelancy reasons
            CollectionAssert.AreEquivalent(
                results.Select(x => x.Pc1), 
                new [] 
                {
                    Data.PersonClasses.JohnArchery,
                    Data.PersonClasses.JohnTennis,
                    Data.PersonClasses.MaryTennis
                });

            foreach (var r in results)
            {
                CollectionAssert.AreEquivalent(
                    r.Pc2,
                    Data.PersonClasses.Where(p => p.PersonId == r.Pc1.PersonId));
            }
        }
    }
}