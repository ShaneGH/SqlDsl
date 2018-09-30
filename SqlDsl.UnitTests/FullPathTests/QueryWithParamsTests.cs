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
    public class QueryWithParamsTests : FullPathTestBase
    {
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public IEnumerable<PersonClass> PersonClasses { get; set; }
            public IEnumerable<Class> Classes { get; set; }
            public IEnumerable<ClassTag> ClassTags { get; set; }
            public IEnumerable<Tag> Tags { get; set; }
        }

        public class Arguments
        {
            public int AValue;
        }

        [Test]
        public async Task QueryWithArgs_WithArgsInWhere_ExecutesCorrectly()
        {
            // arrange
            var query = Sql.Query
                .Sqlite<Arguments, JoinedQueryClass>()
                .From(x => x.ThePerson)
                .LeftJoin(x => x.PersonClasses)
                    .On((q, x) => x.PersonId == q.ThePerson.Id)
                .Where((x, args) => x.PersonClasses.One().ClassId == args.AValue)
                .Compile();

            // act
            var result = await query.ExecuteAsync(Executor, new Arguments { AValue = Data.Classes.Tennis.Id });

            // assert
            Assert.AreEqual(2, result.Count());
            
            Assert.AreEqual(Data.People.John, result.First().ThePerson);
            Assert.AreEqual(1, result.First().PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis, result.First().PersonClasses.First());

            Assert.AreEqual(Data.People.Mary, result.ElementAt(1).ThePerson);
            Assert.AreEqual(1, result.ElementAt(1).PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, result.ElementAt(1).PersonClasses.First());
        }
    }
}
