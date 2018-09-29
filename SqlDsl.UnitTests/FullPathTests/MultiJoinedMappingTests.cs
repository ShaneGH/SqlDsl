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
    public class MultiJoinedMappingTests : FullPathTestBase
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

        // [Test]
        // public async Task SimpleMapOn1Table()
        // {
        //     // arrange
        //     // act
        //     var data = await Sql.Query.Sqlite<Person>()
        //         .From()
        //         .Map(p => new SimpleMapClass
        //         { 
        //             TheName = p.Name,
        //             Inner = new SimpleMapClass
        //             {
        //                 TheName = p.Name
        //             }
        //         })
        //         .ExecuteAsync(Executor);

        //     // assert
        //     Assert.AreEqual(2, data.Count());
        //     Assert.AreEqual(Data.People.John.Name, data.First().TheName);
        //     Assert.AreEqual(Data.People.John.Name, data.First().Inner.TheName);
        //     Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1).TheName);
        //     Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1).Inner.TheName);
        // }
    }
}
