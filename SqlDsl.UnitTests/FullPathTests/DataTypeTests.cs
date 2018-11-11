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

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class DataTypeTests : FullPathTestBase
    {
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public List<PersonClass> PersonClasses { get; set; }
            public List<Class> Classes { get; set; }
            public List<ClassTag> ClassTags { get; set; }
            public List<Tag> Tags { get; set; }
            public List<Purchase> Purchases { get; set; }
        }

        [Test]
        public async Task SimpleMapOn1Table()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<Purchase>()
                .From()
                .Where(p => p.Id == Data.Purchases.JohnPurchasedHimselfShoes.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.Purchases.JohnPurchasedHimselfShoes, data.First());
        }

        class ArrayDataTypeQuery
        {
            // warning CS0649: Field 'PersonClasses/PersonsData' is never assigned to, and will always have its default value null
            #pragma warning disable 0649
            public Person Person;
            public IEnumerable<PersonsData> PersonsData;
            public IEnumerable<PersonClass> Classes;
            #pragma warning restore 0649
        }

        class ArrayDataType1Result
        {
            public long[] ClassIds;
            public byte[] Data;
        }

        class ArrayDataType1_1Result
        {
            public long[] ClassIds;
            public List<byte> Data;
        }

        class ArrayDataType2Result
        {
            public long[] ClassIds;
            public byte[][] Data;
        }

        // class ExploratoryResult
        // {
        //     public byte[] Data;
        //     public PersonClass[] Classes;
        // }

        [Test]
        [Ignore("TODO: not sure what test class this should go in")]
        public void Exploratory()
        {
            // // arrange
            // // act
            // var data = await Sql.Query.Sqlite<ArrayDataTypeQuery>()
            //     .From(x => x.Person)
            //     .InnerJoin(x => x.PersonsData)
            //         .On((q, pd) => q.Person.Id == pd.PersonId)
            //     .InnerJoin(x => x.Classes)
            //         .On((q, pc) => q.Person.Id == pc.PersonId)
            //     .Where(p => p.Person.Id == Data.People.John.Id)
            //     .Map(x => new ExploratoryResult
            //     {
            //         Data = x.PersonsData.Data,

            //         // this part is the exploratory part
            //         Classes = x.Classes.ToArray()
            //     })
            //     .ExecuteAsync(Executor, logger: Logger);

            // assert
            Assert.Fail("Do asserts");
        }

        Task<IEnumerable<ArrayDataType1Result>> ADT1()
        {
            return Sql.Query.Sqlite<ArrayDataTypeQuery>()
                .From(x => x.Person)
                .InnerJoin(x => x.PersonsData)
                    .On((q, pd) => q.Person.Id == pd.PersonId)
                .InnerJoin(x => x.Classes)
                    .On((q, pc) => q.Person.Id == pc.PersonId)
                .Where(p => p.Person.Id == Data.People.John.Id)
                .Map(x => new ArrayDataType1Result
                {
                    Data = x.PersonsData.One().Data,
                    ClassIds = x.Classes.Select(c => c.ClassId).ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);
        }

        [Test]
        public async Task ArrayDataType1xxx()
        {
            // arrange
            // act
            var data = await ADT1();

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            CollectionAssert.AreEqual(Data.PeoplesData.JohnsData.Data, john.Data);

            Assert.AreEqual(2, john.ClassIds.Length);
            Assert.AreEqual(Data.Classes.Tennis.Id, john.ClassIds[0]);
            Assert.AreEqual(Data.Classes.Archery.Id, john.ClassIds[1]);
        }

        [Test]
        public async Task ArrayDataType1_DoesntLogWarning()
        {
            // arrange
            // act
            await ADT1();

            // assert
            Assert.IsEmpty(Logger.WarningMessages);
        }

        Task<IEnumerable<ArrayDataType1_1Result>> ADT1_ConvertArrayToList()
        {
            return Sql.Query.Sqlite<ArrayDataTypeQuery>()
                .From(x => x.Person)
                .InnerJoin(x => x.PersonsData)
                    .On((q, pd) => q.Person.Id == pd.PersonId)
                .InnerJoin(x => x.Classes)
                    .On((q, pc) => q.Person.Id == pc.PersonId)
                .Where(p => p.Person.Id == Data.People.John.Id)
                .Map(x => new ArrayDataType1_1Result
                {
                    Data = x.PersonsData.One().Data.ToList(),
                    ClassIds = x.Classes.Select(c => c.ClassId).ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);
        }

        [Test]
        public async Task ArrayDataType1_ConvertArrayToList()
        {
            // arrange
            // act
            var data = await ADT1_ConvertArrayToList();

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            CollectionAssert.AreEqual(Data.PeoplesData.JohnsData.Data, john.Data.ToArray());

            Assert.AreEqual(2, john.ClassIds.Length);
            Assert.AreEqual(Data.Classes.Tennis.Id, john.ClassIds[0]);
            Assert.AreEqual(Data.Classes.Archery.Id, john.ClassIds[1]);
        }

        [Test]
        public async Task ArrayDataType1_ConvertArrayToList_LogsWarning()
        {
            // arrange
            // act
            await ADT1_ConvertArrayToList();

            // assert
            Logger.WarningMessages.ForEach(Console.WriteLine);
            Assert.AreEqual(1, Logger.WarningMessages.Count);
        }

        Task<IEnumerable<ArrayDataType2Result>> ADT2()
        {
            return Sql.Query.Sqlite<ArrayDataTypeQuery>()
                .From(x => x.Person)
                .InnerJoin(x => x.PersonsData)
                    .On((q, pd) => q.Person.Id == pd.PersonId)
                .InnerJoin(x => x.Classes)
                    .On((q, pc) => q.Person.Id == pc.PersonId)
                .Where(p => p.Person.Id == Data.People.John.Id)
                .Map(x => new ArrayDataType2Result
                {
                    Data = x.PersonsData.Select(d => d.Data).ToArray(),
                    ClassIds = x.Classes.Select(c => c.ClassId).ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);
        }

        [Test]
        public async Task ArrayDataType2()
        {
            // arrange
            // act
            var data = await ADT2();

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();

            Assert.AreEqual(1, john.Data.Length);
            CollectionAssert.AreEqual(Data.PeoplesData.JohnsData.Data, john.Data[0]);

            Assert.AreEqual(2, john.ClassIds.Length);
            Assert.AreEqual(Data.Classes.Tennis.Id, john.ClassIds[0]);
            Assert.AreEqual(Data.Classes.Archery.Id, john.ClassIds[1]);
        }

        [Test]
        public async Task ArrayDataType2_DoesNotWarn()
        {
            // arrange
            // act
            await ADT2();

            // assert
            Assert.IsEmpty(Logger.WarningMessages);
        }

        Task<IEnumerable<List<byte>>> ADT3()
        {
            return Sql.Query.Sqlite<ArrayDataTypeQuery>()
                .From(x => x.Person)
                .InnerJoin(x => x.PersonsData)
                    .On((q, pd) => q.Person.Id == pd.PersonId)
                .Where(p => p.Person.Id == Data.People.John.Id)
                .Map(p => p.PersonsData.One().Data.ToList())
                .ToIEnumerableAsync(Executor, logger: Logger);
        }

        [Test]
        public async Task ArrayDataType3()
        {
            // arrange
            // act
            var data = await ADT3();

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();

            CollectionAssert.AreEqual(Data.PeoplesData.JohnsData.Data, john);
        }

        [Test]
        public async Task ArrayDataType3_LogsWarning()
        {
            // arrange
            // act
            await ADT3();

            // assert
            Assert.IsNotEmpty(Logger.WarningMessages);
        }

        Task<IEnumerable<byte[]>> ADT4()
        {
            return Sql.Query.Sqlite<ArrayDataTypeQuery>()
                .From(x => x.Person)
                .InnerJoin(x => x.PersonsData)
                    .On((q, pd) => q.Person.Id == pd.PersonId)
                .Where(p => p.Person.Id == Data.People.John.Id)
                .Map(p => p.PersonsData.One().Data.ToArray())
                .ToIEnumerableAsync(Executor, logger: Logger);
        }

        [Test]
        public async Task ArrayDataType4()
        {
            // arrange
            // act
            var data = await ADT4();

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();

            CollectionAssert.AreEqual(Data.PeoplesData.JohnsData.Data, john);
        }

        [Test]
        public async Task ArrayDataType4_DoesNotWarn()
        {
            // arrange
            // act
            await ADT4();

            // assert
            CollectionAssert.IsEmpty(Logger.WarningMessages);
        }

        class ArrayDataType3Result
        {
            public long[] ClassIds;
            public List<List<byte>> Data;
        }

        Task<IEnumerable<ArrayDataType3Result>> ADT5()
        {
            // arrange
            // act
            return Sql.Query.Sqlite<ArrayDataTypeQuery>()
                .From(x => x.Person)
                .InnerJoin(x => x.PersonsData)
                    .On((q, pd) => q.Person.Id == pd.PersonId)
                .InnerJoin(x => x.Classes)
                    .On((q, pc) => q.Person.Id == pc.PersonId)
                .Where(p => p.Person.Id == Data.People.John.Id)
                .Map(x => new ArrayDataType3Result
                {
                    Data = x.PersonsData.Select(d => d.Data.ToList()).ToList(),
                    ClassIds = x.Classes.Select(c => c.ClassId).ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);
        }

        [Test]
        public async Task ArrayDataType5()
        {
            // arrange
            // act
            var data = await ADT5();

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();

            Assert.AreEqual(1, john.Data.Count);
            CollectionAssert.AreEqual(Data.PeoplesData.JohnsData.Data, john.Data[0]);

            Assert.AreEqual(2, john.ClassIds.Length);
            Assert.AreEqual(Data.Classes.Tennis.Id, john.ClassIds[0]);
            Assert.AreEqual(Data.Classes.Archery.Id, john.ClassIds[1]);
        }

        [Test]
        public async Task ArrayDataType5_LogsWarning()
        {
            // arrange
            // act
            var data = await ADT5();

            // assert
            CollectionAssert.IsNotEmpty(Logger.WarningMessages);
        }

        Task<IEnumerable<IEnumerable<byte[]>>> ADT8()
        {
            return Sql.Query.Sqlite<ArrayDataTypeQuery>()
                .From(x => x.Person)
                .InnerJoin(x => x.PersonsData)
                    .On((q, pd) => pd.PersonId > -1)
                .Where(p => p.Person.Id == Data.People.John.Id)
                .Map(p => p.PersonsData.Select(pd => pd.Data))
                .ToIEnumerableAsync(Executor, logger: Logger);
        }

        [Test]
        public async Task ArrayDataType8()
        {
            // arrange
            // act
            var data = await ADT8();

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();

            CollectionAssert.AreEqual(new [] 
            { 
                Data.PeoplesData.JohnsData.Data,
                Data.PeoplesData.MarysData.Data 
            }, john);
        }

        [Test]
        public async Task ArrayDataType8_DoesNotLogWarning()
        {
            // arrange
            // act
            foreach (var x in await ADT8())
                foreach (var y in x.Enumerate());

            // assert
            Assert.IsEmpty(Logger.WarningMessages);
        }

        Task<IEnumerable<IEnumerable<byte[]>>> ADT8_1()
        {
            return Sql.Query.Sqlite<ArrayDataTypeQuery>()
                .From(x => x.Person)
                .InnerJoin(x => x.PersonsData)
                    .On((q, pd) => pd.PersonId == q.Person.Id)
                .Where(p => p.Person.Id == Data.People.John.Id)
                .Map(p => p.PersonsData.Select(pd => pd.Data))
                .ToIEnumerableAsync(Executor, logger: Logger);
        }

        [Test]
        public async Task ArrayDataType8_1()
        {
            // arrange
            // act
            var data = await ADT8_1();

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();

            CollectionAssert.AreEqual(new [] 
            { 
                Data.PeoplesData.JohnsData.Data 
            }, john);
        }

        [Test]
        public async Task ArrayDataType8_1_DoesNotLogWarning()
        {
            // arrange
            // act
            foreach (var x in await ADT8_1())
                foreach (var y in x.Enumerate());

            // assert
            Assert.IsEmpty(Logger.WarningMessages);
        }

        Task<IEnumerable<IEnumerable<List<byte>>>> ADT9()
        {
            return Sql.Query.Sqlite<ArrayDataTypeQuery>()
                .From(x => x.Person)
                .InnerJoin(x => x.PersonsData)
                    .On((q, pd) => q.Person.Id == pd.PersonId)
                .Where(p => p.Person.Id == Data.People.John.Id)
                .Map(p => p.PersonsData.Select(pd => pd.Data.ToList()))
                .ToIEnumerableAsync(Executor, logger: Logger);
        }

        [Test]
        public async Task ArrayDataType9()
        {
            // arrange
            // act
            var data = await ADT9();

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();

            CollectionAssert.AreEqual(new [] 
            { 
                Data.PeoplesData.JohnsData.Data 
            }, john);
        }

        [Test]
        public async Task ArrayDataType9_LogsWarning()
        {
            // arrange
            // act
            await ADT9();

            // assert
            Assert.IsNotEmpty(Logger.WarningMessages);
        }

        [Test]
        public void ArrayDataType_JoinReturnsTooMany_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(
                typeof(InvalidOperationException), 
                () => Sql.Query.Sqlite<ArrayDataTypeQuery>()
                    .From(x => x.Person)
                    .InnerJoin(x => x.PersonsData)
                        .On((q, pd) => pd.PersonId < 100)
                    .InnerJoin(x => x.Classes)
                        .On((q, pc) => q.Person.Id == pc.PersonId)
                    .Where(p => p.Person.Id == Data.People.John.Id)
                    .Map(x => new ArrayDataType1Result
                    {
                        Data = x.PersonsData.One().Data,
                        ClassIds = x.Classes.Select(c => c.ClassId).ToArray()
                    })
                    .ToIEnumerableAsync(Executor));
        }
    }
}
