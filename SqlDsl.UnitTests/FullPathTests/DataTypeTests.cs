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
                .ExecuteAsync(Executor);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.Purchases.JohnPurchasedHimselfShoes, data.First());
        }

        class ArrayDataTypeQuery
        {
            public Person Person;
            public IEnumerable<PersonsData> PersonsData;
            public IEnumerable<PersonClass> Classes;
        }

        class ExploratoryResult
        {
            public byte[] Data;
            public PersonClass[] Classes;
        }

        class ArrayDataType1Result
        {
            public int[] ClassIds;
            public byte[] Data;
        }

        class ArrayDataType2Result
        {
            public int[] ClassIds;
            public byte[][] Data;
        }

        [Test]
        [Ignore("TODO: not sure what test class this should go in")]
        public async Task Exploratory()
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
            //     .ExecuteAsync(Executor);

            // assert
            Assert.Fail("Do asserts");
        }

        [Test]
        public async Task ArrayDataType1()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<ArrayDataTypeQuery>()
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
                .ExecuteAsync(Executor);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            CollectionAssert.AreEqual(Data.PeoplesData.JohnsData.Data, john.Data);

            Assert.AreEqual(2, john.ClassIds.Length);
            Assert.AreEqual(Data.Classes.Tennis.Id, john.ClassIds[0]);
            Assert.AreEqual(Data.Classes.Archery.Id, john.ClassIds[1]);
        }

        [Test]
        public async Task ArrayDataType2()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<ArrayDataTypeQuery>()
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
                .ExecuteAsync(Executor);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();

            Assert.AreEqual(1, john.Data.Length);
            CollectionAssert.AreEqual(Data.PeoplesData.JohnsData.Data, john.Data[0]);

            Assert.AreEqual(2, john.ClassIds.Length);
            Assert.AreEqual(Data.Classes.Tennis.Id, john.ClassIds[0]);
            Assert.AreEqual(Data.Classes.Archery.Id, john.ClassIds[1]);
        }
    }
}
