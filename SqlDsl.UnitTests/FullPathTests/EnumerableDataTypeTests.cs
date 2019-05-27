using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.DataParser;
using SqlDsl.Mapper;
using SqlDsl.UnitTests.FullPathTests.Environment;
using SqlDsl.Utils;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(SqlType.MySql)]
    [SqlTestAttribute(SqlType.Sqlite)]
    public class EnumerableDataTypeTests : FullPathTestBase
    {
        public EnumerableDataTypeTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public List<PersonClass> ThePersonClasses { get; set; }
            public List<Class> TheClasses { get; set; }
            public List<ClassTag> TheClassTags { get; set; }
            public List<Tag> TheTags { get; set; }
            public List<Purchase> ThePurchases { get; set; }
        }

        [Test]
        public async Task SimpleMapOn1Table()
        {
            // arrange
            // act
            var data = await Query<Purchase>()
                .Where(p => p.Id == Data.Purchases.JohnPurchasedHimselfShoes.Id)
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.Purchases.JohnPurchasedHimselfShoes, data.First());
        }

        class ArrayDataTypeQuery
        {
            // warning CS0649: Field 'PersonClasses/PersonsData' is never assigned to, and will always have its default value null
            #pragma warning disable 0649
            public Person ThePerson;
            public IEnumerable<PersonsData> ThePersonsData;
            public IEnumerable<PersonClass> TheClasses;
            #pragma warning restore 0649
        }

        class ArrayDataType1Result
        {
            public long[] ClassIds;
            public byte[] TheData;
        }

        class ArrayDataType1_1Result
        {
            public long[] ClassIds;
            public List<byte> TheData;
        }

        class ArrayDataType2Result
        {
            public long[] ClassIds;
            public byte[][] TheData;
        }

        [Test]
        public void Exploratory()
        {
            // arrange
            // act
            var data = Query<ArrayDataTypeQuery>()
                .From(x => x.ThePerson)
                .InnerJoin(x => x.ThePersonsData)
                    .On((q, pd) => q.ThePerson.Id == pd.PersonId)
                .InnerJoin(x => x.TheClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .Where(p => p.ThePerson.Id == Data.People.John.Id)
                .Map(x => new
                {
                    Data = x.ThePersonsData.One().Data,
                    Classes = x.TheClasses.ToArray()
                })
                .ToList(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            Assert.AreEqual(Data.PeoplesData.JohnsData.Data, data[0].Data);
            CollectionAssert.AreEquivalent(new [] { Data.PersonClasses.JohnTennis, Data.PersonClasses.JohnArchery }, data[0].Classes);
        }

        Task<List<ArrayDataType1Result>> ADT1()
        {
            return Query<ArrayDataTypeQuery>()
                .From(x => x.ThePerson)
                .InnerJoin(x => x.ThePersonsData)
                    .On((q, pd) => q.ThePerson.Id == pd.PersonId)
                .InnerJoin(x => x.TheClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .Where(p => p.ThePerson.Id == Data.People.John.Id)
                .Map(x => new ArrayDataType1Result
                {
                    TheData = x.ThePersonsData.One().Data,
                    ClassIds = x.TheClasses.Select(c => c.ClassId).ToArray()
                })
                .ToListAsync(Executor, logger: Logger);
        }

        [Test]
        public async Task ArrayDataType1()
        {
            // arrange
            // act
            var data = await ADT1();

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            CollectionAssert.AreEqual(Data.PeoplesData.JohnsData.Data, john.TheData);

            Assert.AreEqual(2, john.ClassIds.Length);
            CollectionAssert.AreEquivalent(new []
            {
                Data.Classes.Tennis.Id,
                Data.Classes.Archery.Id
            }, john.ClassIds);
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

        Task<List<ArrayDataType1_1Result>> ADT1_ConvertArrayToList()
        {
            return Query<ArrayDataTypeQuery>()
                .From(x => x.ThePerson)
                .InnerJoin(x => x.ThePersonsData)
                    .On((q, pd) => q.ThePerson.Id == pd.PersonId)
                .InnerJoin(x => x.TheClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .Where(p => p.ThePerson.Id == Data.People.John.Id)
                .Map(x => new ArrayDataType1_1Result
                {
                    TheData = x.ThePersonsData.One().Data.ToList(),
                    ClassIds = x.TheClasses.Select(c => c.ClassId).ToArray()
                })
                .ToListAsync(Executor, logger: Logger);
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
            CollectionAssert.AreEqual(Data.PeoplesData.JohnsData.Data, john.TheData.ToArray());

            Assert.AreEqual(2, john.ClassIds.Length);
            CollectionAssert.AreEquivalent(new []
            {
                Data.Classes.Tennis.Id,
                Data.Classes.Archery.Id
            }, john.ClassIds);
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

        Task<List<ArrayDataType2Result>> ADT2()
        {
            return Query<ArrayDataTypeQuery>()
                .From(x => x.ThePerson)
                .InnerJoin(x => x.ThePersonsData)
                    .On((q, pd) => q.ThePerson.Id == pd.PersonId)
                .InnerJoin(x => x.TheClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .Where(p => p.ThePerson.Id == Data.People.John.Id)
                .Map(x => new ArrayDataType2Result
                {
                    TheData = x.ThePersonsData.Select(d => d.Data).ToArray(),
                    ClassIds = x.TheClasses.Select(c => c.ClassId).ToArray()
                })
                .ToListAsync(Executor, logger: Logger);
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

            Assert.AreEqual(1, john.TheData.Length);
            CollectionAssert.AreEqual(Data.PeoplesData.JohnsData.Data, john.TheData[0]);

            Assert.AreEqual(2, john.ClassIds.Length);
            CollectionAssert.AreEquivalent(new []
            {
                Data.Classes.Tennis.Id,
                Data.Classes.Archery.Id
            }, john.ClassIds);
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

        Task<List<List<byte>>> ADT3()
        {
            return Query<ArrayDataTypeQuery>()
                .From(x => x.ThePerson)
                .InnerJoin(x => x.ThePersonsData)
                    .On((q, pd) => q.ThePerson.Id == pd.PersonId)
                .Where(p => p.ThePerson.Id == Data.People.John.Id)
                .Map(p => p.ThePersonsData.One().Data.ToList())
                .ToListAsync(Executor, logger: Logger);
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

        Task<List<byte[]>> ADT4()
        {
            return Query<ArrayDataTypeQuery>()
                .From(x => x.ThePerson)
                .InnerJoin(x => x.ThePersonsData)
                    .On((q, pd) => q.ThePerson.Id == pd.PersonId)
                .Where(p => p.ThePerson.Id == Data.People.John.Id)
                .Map(p => p.ThePersonsData.One().Data.ToArray())
                .ToListAsync(Executor, logger: Logger);
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
            public List<List<byte>> TheData;
        }

        Task<List<ArrayDataType3Result>> ADT5()
        {
            // arrange
            // act
            return Query<ArrayDataTypeQuery>()
                .From(x => x.ThePerson)
                .InnerJoin(x => x.ThePersonsData)
                    .On((q, pd) => q.ThePerson.Id == pd.PersonId)
                .InnerJoin(x => x.TheClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .Where(p => p.ThePerson.Id == Data.People.John.Id)
                .Map(x => new ArrayDataType3Result
                {
                    TheData = x.ThePersonsData.Select(d => d.Data.ToList()).ToList(),
                    ClassIds = x.TheClasses.Select(c => c.ClassId).ToArray()
                })
                .ToListAsync(Executor, logger: Logger);
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

            Assert.AreEqual(1, john.TheData.Count);
            CollectionAssert.AreEqual(Data.PeoplesData.JohnsData.Data, john.TheData[0]);

            Assert.AreEqual(2, john.ClassIds.Length);
            CollectionAssert.AreEquivalent(new []
            {
                Data.Classes.Tennis.Id,
                Data.Classes.Archery.Id
            }, john.ClassIds);
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

        Task<List<IEnumerable<byte[]>>> ADT8()
        {
            return Query<ArrayDataTypeQuery>()
                .From(x => x.ThePerson)
                .InnerJoin(x => x.ThePersonsData)
                    .On((q, pd) => pd.PersonId > -1)
                .Where(p => p.ThePerson.Id == Data.People.John.Id)
                .Map(p => p.ThePersonsData.Select(pd => pd.Data))
                .ToListAsync(Executor, logger: Logger);
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

        Task<List<IEnumerable<byte[]>>> ADT8_1()
        {
            return Query<ArrayDataTypeQuery>()
                .From(x => x.ThePerson)
                .InnerJoin(x => x.ThePersonsData)
                    .On((q, pd) => pd.PersonId == q.ThePerson.Id)
                .Where(p => p.ThePerson.Id == Data.People.John.Id)
                .Map(p => p.ThePersonsData.Select(pd => pd.Data))
                .ToListAsync(Executor, logger: Logger);
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

        Task<List<IEnumerable<List<byte>>>> ADT9()
        {
            return Query<ArrayDataTypeQuery>()
                .From(x => x.ThePerson)
                .InnerJoin(x => x.ThePersonsData)
                    .On((q, pd) => q.ThePerson.Id == pd.PersonId)
                .Where(p => p.ThePerson.Id == Data.People.John.Id)
                .Map(p => p.ThePersonsData.Select(pd => pd.Data.ToList()))
                .ToListAsync(Executor, logger: Logger);
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
                typeof(ParsingException), 
                () => Query<ArrayDataTypeQuery>()
                    .From(x => x.ThePerson)
                    .InnerJoin(x => x.ThePersonsData)
                        .On((q, pd) => pd.PersonId < 100)
                    .InnerJoin(x => x.TheClasses)
                        .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                    .Where(p => p.ThePerson.Id == Data.People.John.Id)
                    .Map(x => new ArrayDataType1Result
                    {
                        TheData = x.ThePersonsData.One().Data,
                        ClassIds = x.TheClasses.Select(c => c.ClassId).ToArray()
                    })
                    .ToListAsync(Executor));
        }

        [Test]
        public void ArrayDataType_WithReContext_SomethingHappens()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(SqlBuilderException), () =>
                Query<ArrayDataTypeQuery>()
                    .From(x => x.ThePerson)
                    .InnerJoin(x => x.ThePersonsData)
                        .On((q, pd) => q.ThePerson.Id == pd.PersonId)
                    .Where(p => p.ThePerson.Id == Data.People.John.Id)
                    .Map(p => p.ThePersonsData.One().Data.Select(pd => (int)pd))
                    .ToList(Executor, logger: Logger));
        }

        [Test]
        public void ArrayDataType_WithOneOnData_SomethingHappens()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(ParsingException), () =>
                Query<ArrayDataTypeQuery>()
                    .From(x => x.ThePerson)
                    .InnerJoin(x => x.ThePersonsData)
                        .On((q, pd) => q.ThePerson.Id == pd.PersonId)
                    .Where(p => p.ThePerson.Id == Data.People.John.Id)
                    .Map(p => p.ThePersonsData.One().Data.One())
                    .ToList(Executor, logger: Logger));
        }
    }
}
