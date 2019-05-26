using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(SqlType.MySql)]
    public class OrderByTests : FullPathTestBase
    {
        public OrderByTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        [Test]
        public async Task OrderBy()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType)
                .OrderBy(x => x.ThePerson.Id)
                .Map(q => q.ThePerson.Name)
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.John.Name, Data.People.Mary.Name }, data);
        }

        [Test]
        public async Task OrderByDescending()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType)
                .OrderByDesc(x => x.ThePerson.Id)
                .Map(q => q.ThePerson.Name)
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.Mary.Name, Data.People.John.Name }, data);
        }

        [Test]
        public async Task OrderBy_ThenBy()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType)
                .OrderBy(x => x.ThePerson.Id)
                .ThenBy(x => x.TheClasses.One().Id)
                .Map(q => new 
                {
                    name = q.ThePerson.Name,
                    classes = q.TheClasses
                        .Select(c => c.Name)
                })
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.John.Name, Data.People.Mary.Name }, data.Select(x => x.name));
            CollectionAssert.AreEqual(new[] { Data.Classes.Tennis.Name, Data.Classes.Archery.Name, Data.Classes.Tennis.Name }, data.SelectMany(x => x.classes));
        }

        [Test]
        public async Task OrderByDesc_ThenBy()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType)
                .OrderByDesc(x => x.ThePerson.Id)
                .ThenBy(x => x.TheClasses.One().Id)
                .Map(q => new 
                {
                    name = q.ThePerson.Name,
                    classes = q.TheClasses
                        .Select(c => c.Name)
                })
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.Mary.Name, Data.People.John.Name }, data.Select(x => x.name));
            CollectionAssert.AreEqual(new[] { Data.Classes.Tennis.Name, Data.Classes.Tennis.Name, Data.Classes.Archery.Name }, data.SelectMany(x => x.classes));
        }

        [Test]
        public async Task OrderByDesc_ThenByDesc()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType)
                .OrderByDesc(x => x.ThePerson.Id)
                .ThenByDesc(x => x.TheClasses.One().Id)
                .Map(q => new 
                {
                    name = q.ThePerson.Name,
                    classes = q.TheClasses
                        .Select(c => c.Name)
                })
                .ToArrayAsync(Executor, logger: Logger);
                
            // assert
            CollectionAssert.AreEqual(new[] { Data.People.Mary.Name, Data.People.John.Name }, data.Select(x => x.name));
            CollectionAssert.AreEqual(new[] { Data.Classes.Tennis.Name, Data.Classes.Archery.Name, Data.Classes.Tennis.Name }, data.SelectMany(x => x.classes));
        }

        [Test]
        public async Task OrderBy_ThenByDesc()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(SqlType)
                .OrderBy(x => x.ThePerson.Id)
                .ThenByDesc(x => x.TheClasses.Select(y => y.Id))
                .Map(q => new 
                {
                    name = q.ThePerson.Name,
                    classes = q.TheClasses
                        .Select(c => c.Name)
                })
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.John.Name, Data.People.Mary.Name }, data.Select(x => x.name));
            CollectionAssert.AreEqual(
                new[] { Data.Classes.Archery.Name, Data.Classes.Tennis.Name, Data.Classes.Tennis.Name }, 
                data.SelectMany(x => x.classes));
        }

        [Test]
        public async Task OrderBy_WithBinaryCondition()
        {
            // arrange
            // act
            var data = await Query<PersonClass>()
                .OrderByDesc(x => x.PersonId + x.ClassId)
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                Data.PersonClasses.JohnArchery,
                Data.PersonClasses.MaryTennis,
                Data.PersonClasses.JohnTennis
            }, data);
        }

        [Test]
        public async Task OrderBy_WithArgsCondition()
        {
            // arrange
            // act
            var data = await Query<int, PersonClass>()
                .OrderByDesc((x, a) => x.PersonId + a)
                .ToArrayAsync(Executor, 7, logger: Logger);

            // assert
            Assert.AreEqual(3, data.Length);
        }
    }
}
