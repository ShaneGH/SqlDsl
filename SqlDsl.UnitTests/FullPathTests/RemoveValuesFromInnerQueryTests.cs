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
using SqlDsl.Dsl;

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class RemoveValuesFromInnerQueryTests : FullPathTestBase
    {
        [Test]
        public async Task OrderByTableNotInMap()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery()
                .OrderBy(x => x.TheTags.One().Id)
                .Map(q => q.ThePerson.Name)
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.John.Name, Data.People.Mary.Name }, data);
        }

        [Test]
        public async Task WhereTableNotInMap()
        {
            // arrange
            // act
            var data = await TestUtils.FullyJoinedQuery()
                .Where(x => x.TheClasses.One().Id == Data.Classes.Archery.Id)
                .Map(q => q.ThePerson.Name)
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.John.Name }, data);
        }

        [Test]
        public void RemovesUnusedTablesForNonStrictJoin()
        {
            // arrange
            // act
            var data = Sql.Query.Sqlite<QueryContainer>(false)
                .From(x => x.ThePerson)
                .InnerJoin(q => q.ThePersonsData)
                    .On((q, pc) => q.ThePerson.Id == 77)
                .Map(p => p.ThePerson.Name)
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEquivalent(new[]{ Data.People.John.Name, Data.People.Mary.Name  }, data);
        }

        [Test]
        public void RetainsUnusedTablesForStrictJoin()
        {
            // arrange
            // act
            var data = Sql.Query.Sqlite<QueryContainer>()
                .From(x => x.ThePerson)
                .InnerJoin(q => q.ThePersonsData)
                    .On((q, pc) => q.ThePerson.Id == 77)
                .Map(p => p.ThePerson.Name)
                .ToArray(Executor, logger: Logger);

            // assert
            Assert.AreEqual(0, data.Length);
        }
    }
}
