using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;
using NUnit.Framework;
using SqlDsl.Utils;
using SqlDsl.UnitTests.FullPathTests.Environment;
using SqlDsl.Sqlite;
using NUnit.Framework.Interfaces;
using SqlDsl.ObjectBuilders;
using System.Linq.Expressions;

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class MappingTestsWithFunLogic : FullPathTestBase
    {

        class PreMapped 
        {
            public long ClassId;
            public long PersonId;
            public PreMapped AnotherPreMapped;

            public PreMapped(){}

            public PreMapped(PreMapped anotherPreMapped) { AnotherPreMapped = anotherPreMapped; }

            public PreMapped(long personId) { PersonId = personId; }
        }

        [Test]
        public async Task ReturnMultipleFromMap_PreMappedWithSimpleProperty()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => p.ThePersonClasses.Select(pc => new PreMapped { ClassId = pc.ClassId }))
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count(), "1");
            Assert.AreEqual(2, data.ElementAt(0).Count(), "2");
            Assert.AreEqual(1, data.ElementAt(1).Count(), "3");
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data.ElementAt(0).ElementAt(0).ClassId, "4");
            Assert.AreEqual(Data.PersonClasses.JohnArchery.ClassId, data.ElementAt(0).ElementAt(1).ClassId, "5");
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data.ElementAt(1).ElementAt(0).ClassId, "6");
        }

        [Test]
        public async Task ReturnMultipleFromMap_PreMappedWithSimpleConstructorArg()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => p.ThePersonClasses.Select(pc => new PreMapped(pc.PersonId)).ToList())
                .ToListAsync(Executor, logger: Logger);

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
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => p.ThePersonClasses
                    .Select(pc => new PreMapped(pc.PersonId)
                    { 
                        ClassId = pc.ClassId
                    })
                    .ToList())
                .ToListAsync(Executor, logger: Logger);

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

        // See todo in ComplexMapBuilder.BuildMapForConstructor

        // [Test]
        // public async Task ReturnMultipleFromMap_PreMappedWithComplexProperty()
        // {
        //     // arrange
        //     // act
        //     var data = await FullyJoinedQuery
        //        .Build()
        //         .Map(p => p.PersonClasses.Select(pc => new PreMapped { AnotherPreMapped = new PreMapped() }).ToList())
        //         .ToListAsync(Executor, logger: Logger);

        //     // assert
        //     Assert.AreEqual(2, data.Count, "1");
        //     Assert.AreEqual(2, data[0].Count(), "2");
        //     Assert.AreEqual(1, data[1].Count(), "3");
        //     Assert.NotNull(data[0][0].AnotherPreMapped, "4");
        //     Assert.NotNull(data[0][1].AnotherPreMapped, "5");
        //     Assert.NotNull(data[1][0].AnotherPreMapped, "6");
        // }

        // [Test]
        // public async Task ReturnMultipleFromMap_PreMappedWithSimpleConstructorArg()
        // {
        //     // arrange
        //     // act
        //     var data = await FullyJoinedQuery<object>()
        //         .Map(p => p.PersonClasses.Select(pc => new PreMapped(pc.PersonId)).ToList())
        //         .ToListAsync(Executor, logger: Logger);

        //    // assert may not be 100% correct

        //     // assert
        //     Assert.AreEqual(2, data.Count, "1");
        //     Assert.AreEqual(2, data[0].Count(), "2");
        //     Assert.AreEqual(1, data[1].Count(), "3");
        //     Assert.AreEqual(Data.PersonClasses.JohnTennis.PersonId, data[0][0].PersonId, "4");
        //     Assert.AreEqual(Data.PersonClasses.JohnArchery.PersonId, data[0][1].PersonId, "5");
        //     Assert.AreEqual(Data.PersonClasses.MaryTennis.PersonId, data[1][0].PersonId, "6");
        // }

        // [Test]
        // public async Task ReturnMultipleFromMap_PreMappedWithSimplePropertyAndSimpleConstructorArg()
        // {
        //     // arrange
        //     // act
        //     var data = await FullyJoinedQuery<object>()
        //         .Map(p => p.PersonClasses.Select(pc => new PreMapped(pc.PersonId) { ClassId = pc.ClassId }).ToList())
        //         .ToListAsync(Executor, logger: Logger);

        //    // assert may not be 100% correct

        //     // assert
        //     Assert.AreEqual(2, data.Count, "1");
        //     Assert.AreEqual(2, data[0].Count(), "2");
        //     Assert.AreEqual(1, data[1].Count(), "3");
        //     Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data[0][0].ClassId, "4 1");
        //     Assert.AreEqual(Data.PersonClasses.JohnTennis.PersonId, data[0][0].PersonId, "4 2");
        //     Assert.AreEqual(Data.PersonClasses.JohnArchery.ClassId, data[0][1].ClassId, "5 1");
        //     Assert.AreEqual(Data.PersonClasses.JohnArchery.PersonId, data[0][1].PersonId, "5 2");
        //     Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data[1][0].ClassId, "6 1");
        //     Assert.AreEqual(Data.PersonClasses.MaryTennis.PersonId, data[1][0].PersonId, "6 2");
        // }

        // [Test]
        // public async Task ReturnMultipleFromMap_PreMappedWithNoPropertiesOrConstructorArgs()
        // {
        //     // arrange
        //     // act
        //     var data = await FullyJoinedQuery<object>()
        //         .Map(p => p.PersonClasses.Select(pc => new PreMapped()).ToList())
        //         .ToListAsync(Executor, logger: Logger);

        //     // assert
        //     Assert.AreEqual(2, data.Count, "1");
        //     Assert.AreEqual(2, data[0].Count(), "2");
        //     Assert.AreEqual(1, data[1].Count(), "3");
        // }

        [Test]
        public async Task ReturnOneSubPropFromMap()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery()
                .Where(q => q.ThePersonClasses.One().ClassId == Data.Classes.Tennis.Id)
                .Map(p => p.ThePersonClasses.One().ClassId)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data.First());
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data.ElementAt(1));
        }

        [Test]
        public async Task ReturnOneSubPropFromMap2()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery()
                .Where(q => q.ThePersonClasses.One().ClassId == Data.Classes.Tennis.Id)
                .Map(p => p.ThePersonClasses.Select(x => x.ClassId).One())
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data.First());
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data.ElementAt(1));
        }

        [Test]
        public async Task ReturnMultipleSubPropsFromMap()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => p.ThePersonClasses.Select(pc => pc.ClassId))
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(2, data.First().Count());
            Assert.AreEqual(1, data.ElementAt(1).Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data.First().First());
            Assert.AreEqual(Data.PersonClasses.JohnArchery.ClassId, data.First().ElementAt(1));
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data.ElementAt(1).First());
        }

        [Test]
        public async Task MapAndReturnConstant()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => 77)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First());
            Assert.AreEqual(77, data.ElementAt(1));
        }

        [Test]
        public async Task MapAndReturnMappedConstant()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => new Person { Id = 77 })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First().Id);
            Assert.AreEqual(77, data.ElementAt(1).Id);
        }
    }
}
