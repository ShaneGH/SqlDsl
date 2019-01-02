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
    public class PagingTests : FullPathTestBase
    {
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public List<PersonClass> ThePersonClasses { get; set; }
            public List<Class> TheClasses { get; set; }
            public List<ClassTag> TheClassTags { get; set; }
            public List<Tag> TheTags { get; set; }
        }

        IQuery<object, JoinedQueryClass> FullyJoinedQuery() => FullyJoinedQuery<object>();

        IQuery<TArgs, JoinedQueryClass> FullyJoinedQuery<TArgs>()
        {
            return Sql.Query.Sqlite<TArgs, JoinedQueryClass>()
                .From(result => result.ThePerson)
                .LeftJoin<PersonClass>(result => result.ThePersonClasses)
                    .On((r, pc) => r.ThePerson.Id == pc.PersonId)
                .LeftJoin<Class>(result => result.TheClasses)
                    .On((r, pc) => r.ThePersonClasses.One().ClassId == pc.Id)
                .LeftJoin<ClassTag>(result => result.TheClassTags)
                    .On((r, pc) => r.TheClasses.One().Id == pc.ClassId)
                .LeftJoin<Tag>(result => result.TheTags)
                    .On((r, pc) => r.TheClassTags.One().TagId == pc.Id);
        }

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
                // results here are not correct
                // (second is probably correct)
                Data.ClassTags.TennisBallSport,
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
            var data = FullyJoinedQuery()
                .Map(x => new
                {
                    person = x.ThePerson.Name,
                    classes = x.TheClasses.Count
                })
                .Skip(1)
                .Take(20)
                .ToArray(Executor, null, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                new { person = Data.People.John.Name, classes = 2 },
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
            var data = FullyJoinedQuery<(int skip, int take)>()
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
                new { person = Data.People.John.Name, classes = 2 },
            }, data);
        }
    }
}
