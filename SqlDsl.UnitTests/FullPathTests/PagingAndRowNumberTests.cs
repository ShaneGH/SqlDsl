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
    public class PagingAndRowNumberTests : FullPathTestBase
    {
        [Test]
        public void Take_UnmappedQuery_PagesResults()
        {
            // arrange
            // act
            var data = Sql.Query.Sqlite<ClassTag>()
                .Take(2)
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            {
                Data.ClassTags.ArcherySport,
                Data.ClassTags.TennisSport,
            }, data);
        }

        [Test]
        public void SkipAndTake_UnmappedQuery_PagesResults()
        {
            // arrange
            // act
            var data = Sql.Query.Sqlite<ClassTag>()
                .Skip(1).Take(2)
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                Data.ClassTags.TennisSport,
                Data.ClassTags.TennisBallSport,
            }, data);
        }

        [Test]
        public void SkipAndTake_WithWhere_PagesResults()
        {
            // arrange
            // act
            var data = Sql.Query.Sqlite<ClassTag>()
                .Where(x => x.ClassId != 999)
                .Skip(1).Take(2)
                .ToArray(Executor, logger: Logger);
                
            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                Data.ClassTags.TennisSport,
                Data.ClassTags.TennisBallSport,
            }, data);
        }

        [Test]
        public void SkipAndTake_MappedQuery_PagesResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(false)
                .Map(x => new
                {
                    person = x.ThePerson.Name,
                    classes = x.TheClasses.Count
                })
                .Skip(1)
                .Take(20)
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                new { person = Data.People.Mary.Name, classes = 1 },
            }, data);
        }

        [Test]
        public void SkipAndTake_UnmappedWithArgs_PagesResults()
        {
            // arrange
            // act
            var data = Sql.Query.Sqlite<(int skip, int take), ClassTag>()
                .Skip(a => a.skip)
                .Take(a => a.take)
                .ToArray(Executor, (1, 2), logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                Data.ClassTags.TennisSport,
                Data.ClassTags.TennisBallSport,
            }, data);
        }

        [Test]
        public void SkipAndTake_MappedWithArgs_PagesResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQueryWithArg<(int skip, int take)>(false)
                .Map(x => new
                {
                    person = x.ThePerson.Name,
                    classes = x.TheClasses.Count
                })
                .Skip(a => a.skip)
                .Take(a => a.take)
                .ToArray(Executor, (1, 20), logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                new { person = Data.People.Mary.Name, classes = 1 },
            }, data);
        }

        [Test]
        public void Where_WithRowNumber_PagesResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery(false)
                .Where(x => Sql.RowNumber() == 2)
                .Map(x => new
                {
                    person = x.ThePerson.Name,
                    classes = x.TheClasses.Count
                })
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                new { person = Data.People.Mary.Name, classes = 1 },
            }, data);
        }

        [Test]
        public void Map_WithRowNumber_PagesResults()
        {
            // arrange
            // act
            var data = TestUtils
                .FullyJoinedQuery()
                .Map(x => new
                {
                    person = x.ThePerson,
                    rowNumber = Sql.RowNumber()
                })
                .ToArray(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                new { person = Data.People.John, rowNumber = 1 },
                new { person = Data.People.Mary, rowNumber = 2 },
            }, data);
        }
    }
}
