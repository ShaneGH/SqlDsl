using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(TestFlavour.Sqlite)]
    class MappingObjectWithNoTableDataTests : FullPathTestBase
    {
        public MappingObjectWithNoTableDataTests(TestFlavour testFlavour)
            : base(testFlavour)
        {
        }
        
        class MappedResult 
        {
            public long ClassId;
            public long PersonId;
            public MappedResult AnotherPreMapped;

            public MappedResult(){}

            public MappedResult(MappedResult anotherPreMapped) { AnotherPreMapped = anotherPreMapped; }

            public MappedResult(long personId) { PersonId = personId; }
        }

        // // See todo in ComplexMapBuilder.BuildMapForConstructor

        // [Test]
        // public async Task ReturnMultipleFromMap_PreMappedWithComplexProperty()
        // {
        //     // arrange
        //     // act
        //     var data = await TestUtils.FullyJoinedQuery(TestFlavour)
        //         .Map(p => p.ThePersonClasses.Select(pc => new MappedResult { AnotherPreMapped = new MappedResult() }).ToList())
        //         .ToListAsync(Executor, logger: Logger);

        //     // assert
        //     Assert.AreEqual(2, data.Count, "1");
        //     Assert.AreEqual(2, data[0].Count(), "2");
        //     Assert.AreEqual(1, data[1].Count(), "3");
        //     Assert.NotNull(data[0][0].AnotherPreMapped, "4");
        //     Assert.NotNull(data[0][1].AnotherPreMapped, "5");
        //     Assert.NotNull(data[1][0].AnotherPreMapped, "6");
        // }
        
        // // See todo in ComplexMapBuilder.BuildMapForConstructor

        // [Test]
        // public async Task ReturnMultipleFromMap_PreMappedWithNoPropertiesOrConstructorArgs()
        // {
        //     // arrange
        //     // act
        //     var data = await TestUtils.FullyJoinedQuery(TestFlavour)
        //         .Map(p => p.ThePersonClasses.Select(pc => new MappedResult()).ToList())
        //         .ToListAsync(Executor, logger: Logger);

        //     // assert
        //     Assert.AreEqual(2, data.Count, "1");
        //     Assert.AreEqual(2, data[0].Count(), "2");
        //     Assert.AreEqual(1, data[1].Count(), "3");
        // }

        [Test]
        public async Task ReturnMultipleFromMap_PreMappedWithSimpleConstructorArg()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(TestFlavour)
                .Map(p => p.ThePersonClasses.Select(pc => new MappedResult(pc.PersonId)).ToList())
                .ToListAsync(Executor, logger: Logger);

           // assert may not be 100% correct

            // assert
            Assert.AreEqual(2, data.Count, "1");
            Assert.AreEqual(2, data[0].Count(), "2");
            Assert.AreEqual(1, data[1].Count(), "3");
            Assert.AreEqual(Data.PersonClasses.JohnTennis.PersonId, data[0][0].PersonId, "4");
            Assert.AreEqual(Data.PersonClasses.JohnArchery.PersonId, data[0][1].PersonId, "5");
            Assert.AreEqual(Data.PersonClasses.MaryTennis.PersonId, data[1][0].PersonId, "6");
        }

        [Test]
        public async Task ReturnMultipleFromMap_PreMappedWithSimplePropertyAndSimpleConstructorArg()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery(TestFlavour)
                .Map(p => p.ThePersonClasses.Select(pc => new MappedResult(pc.PersonId) { ClassId = pc.ClassId }).ToList())
                .ToListAsync(Executor, logger: Logger);

           // assert may not be 100% correct

            // assert
            Assert.AreEqual(2, data.Count, "1");
            Assert.AreEqual(2, data[0].Count(), "2");
            Assert.AreEqual(1, data[1].Count(), "3");
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data[0][0].ClassId, "4 1");
            Assert.AreEqual(Data.PersonClasses.JohnTennis.PersonId, data[0][0].PersonId, "4 2");
            Assert.AreEqual(Data.PersonClasses.JohnArchery.ClassId, data[0][1].ClassId, "5 1");
            Assert.AreEqual(Data.PersonClasses.JohnArchery.PersonId, data[0][1].PersonId, "5 2");
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data[1][0].ClassId, "6 1");
            Assert.AreEqual(Data.PersonClasses.MaryTennis.PersonId, data[1][0].PersonId, "6 2");
        }
    }
}