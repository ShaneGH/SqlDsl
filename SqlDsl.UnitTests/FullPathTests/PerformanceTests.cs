using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(SqlType.Sqlite)]
    public class PerformanceTests : FullPathTestBase
    {
        public PerformanceTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        class Cls1
        {
            public string thename;
            public Cls2[] theclasses;
        }

        class Cls2
        {
            public string thename;
            public Cls3[] tags1;
            public Cls3[] tags2;
        }

        class Cls3
        {
            public string tagName;
        }

        [Test]
        public async Task CountObjectGraphAllocations_ForProperties()
        {
            // arrange
            // act
            await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(x => new Cls1
                {
                    thename = x.ThePerson.Name,
                    theclasses = x.TheClasses
                        .Select(cl => new Cls2
                        {
                            thename = cl.Name,
                            tags1 = x.TheTags
                                .Select(z => new Cls3 { tagName = z.Name })
                                .ToArray(),
                            tags2 = x.TheTags
                                .Select(z => new Cls3 { tagName = z.Name })
                                .ToArray()
                        })
                        .ToArray()
                })
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            var debugCount = Logger.DebugMessages
                .Where(m => m.Contains(((int)LogMessages.CreatedObjectGraphAllocation).ToString()))
                .Count();

            Assert.AreEqual(3, debugCount, "3 objects represents 3 levels of properties");
        }

        [Test]
        public async Task CountObjectGraphAllocations_ForConstructorArgs()
        {
            // arrange
            // act
            await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(x => new
                {
                    name = x.ThePerson.Name,
                    classes = x.TheClasses
                        .Select(cl => new
                        {
                            name = cl.Name,
                            tags1 = x.TheTags
                                .Select(z => new { tagName = z.Name })
                                .ToArray(),
                            tags2 = x.TheTags
                                .Select(z => new { tagName = z.Name })
                                .ToArray()
                        })
                        .ToArray()
                })
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            var debugCount = Logger.DebugMessages
                .Where(m => m.Contains(((int)LogMessages.CreatedObjectGraphAllocation).ToString()))
                .Count();

            Assert.AreEqual(3, debugCount, "3 objects represents 3 levels of properties");
        }

        [Test]
        public async Task CountPropMapValueAllocations_ForConstructorArgs()
        {
            // arrange
            // act
            var data= await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(x => x.ThePerson.Name)
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            var debugCount = Logger.DebugMessages
                .Where(m => m.Contains(((int)LogMessages.CreatedPropMapValueAllocation).ToString()))
                .Count();

            Assert.AreEqual(2, data.Length);
            Assert.AreEqual(1, debugCount);
        }
    }
}
