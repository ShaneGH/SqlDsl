using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests.AggregateFunctions
{
    [TestFixture]
    public class GroupByTests : FullPathTestBase
    {

        [Test]
        public async Task GroupBy_WithGroupOn1Table_UsingConstructorArgs()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(false)
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Select(x => x.Id).Count()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 2
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 1
                }
            }, data);
        }

        class CountAndGroupTest
        {
            public string thePerson;
            public int theClasses;
        }

        [Test]
        public async Task GroupBy_WithGroupOn1Table_UsingProperties()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(false)
                .Map(p => new CountAndGroupTest
                {
                    thePerson = p.ThePerson.Name,
                    theClasses = p.TheClasses.Select(x => x.Id).Count()
                })
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Length);
            Assert.AreEqual(Data.People.John.Name, data[0].thePerson);
            Assert.AreEqual(2, data[0].theClasses);
            Assert.AreEqual(Data.People.Mary.Name, data[1].thePerson);
            Assert.AreEqual(1, data[1].theClasses);
        }

        [Test]
        public void GroupBy_GroupIsWithinScopeOfChildJoin()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery()
                .Where(q => q.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    classesWithTags = q.TheClassTags
                        .Select(tag => new 
                        {
                            cls = q.TheClasses.Select(x => x.Id).Count()
                        })
                        .ToArray()
                })
                .ToList(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            Assert.AreEqual(3, data[0].classesWithTags.Length);
            Assert.AreEqual(1, data[0].classesWithTags[0].cls);
            Assert.AreEqual(1, data[0].classesWithTags[1].cls);
            Assert.AreEqual(1, data[0].classesWithTags[2].cls);
        }

        [Test]
        public async Task CountAndGroup_GroupByTableWithNoOtherColumns()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Select(x => new
                    {
                        tags = p.TheTags.Count()
                    }).ToArray()
                })
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Length);
            Assert.AreEqual(Data.People.John.Name, data[0].person);
            Assert.AreEqual(new [] { new { tags = 2 }, new { tags = 1 } }, data[0].classes);
        }

        [Test]
        public void CountAndGroup_TwoAggregateFunctionsOnOneTable()
        {
            // arrange
            // act
            var result = TestUtils
                .FullyJoinedQuery(false)
                .Map(x => new
                {
                    name = x.ThePerson.Name,
                    classesSum = x.TheClasses.Sum(y => y.Id),
                    classesCount = x.TheClasses.Count
                })
                .ToList(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, result.Count);

            var john = result[0];
            Assert.AreEqual(Data.People.John.Name, john.name);
            Assert.AreEqual(2, john.classesCount);
            Assert.AreEqual(7, john.classesSum);

            var mary = result[1];
            Assert.AreEqual(Data.People.Mary.Name, mary.name);
            Assert.AreEqual(1, mary.classesCount);
            Assert.AreEqual(3, mary.classesSum);
        }

        [Test]
        public void CountAndGroup_TwoAggregateFunctionsOnOneColumn()
        {
            // arrange
            // act
            var result = TestUtils
                .FullyJoinedQuery()
                .Map(x => new
                {
                    name = x.ThePerson.Name,
                    classesAverage = x.TheClasses.Sum(y => y.Id) / x.TheClasses.Count
                })
                .ToList(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, result.Count);

            var john = result[0];
            Assert.AreEqual(Data.People.John.Name, john.name);
            Assert.AreEqual(3, john.classesAverage);

            var mary = result[1];
            Assert.AreEqual(Data.People.Mary.Name, mary.name);
            Assert.AreEqual(3, mary.classesAverage);
        }

        [Test]
        public void CountAndGroup_UseTableInGroupAndNonGroup()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(InvalidOperationException), 
                () => TestUtils
                    .FullyJoinedQuery()
                    .Map(x => new
                    {
                        name = x.ThePerson.Name,
                        classes = x.TheClasses.Select(c => c.Name).ToArray(),
                        classesCount = x.TheClasses.Count
                    })
                    .ToList(Executor, logger: Logger));

        }

        [Test]
        public void CountAndGroup_UseTableInGroupAndNonGroupInSameSelectColumn()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(InvalidOperationException), 
                () => TestUtils
                    .FullyJoinedQuery()
                    .Map(x => new
                    {
                        name = x.ThePerson.Name,
                        classes = x.TheClasses
                            .Select(c => c.Id + x.TheClasses.Count)
                            .ToArray()
                    })
                    .ToList(Executor, logger: Logger));
        }

        [Test]
        public void CountAndGroup_UseGroupedColumnAndNonGroupedDecendantColumn()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(InvalidOperationException), 
                () => TestUtils
                    .FullyJoinedQuery()
                    .Map(x => new
                    {
                        name = x.ThePerson.Name,
                        classesCount = x.TheClasses.Count,
                        tags = x.TheTags.Select(c => c.Name).ToArray()
                    })
                    .ToList(Executor, logger: Logger));
        }

        [Test]
        public void CountAndGroup_WithSimpleQuery()
        {
            // arrange
            // act
            var result = TestUtils
                .FullyJoinedQuery(false)
                .Map(x => x.TheClasses.Count)
                .ToList(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[]{ 2, 1 }, result);
        }
    }
}
