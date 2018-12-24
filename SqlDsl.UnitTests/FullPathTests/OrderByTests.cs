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
    public class OrderByTests : FullPathTestBase
    {
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public List<PersonClass> ThePersonClasses { get; set; }
            public List<Class> TheClasses { get; set; }
            public List<ClassTag> TheClassTags { get; set; }
            public List<Tag> TheTags { get; set; }
        }

        IQuery<JoinedQueryClass> FullyJoinedQuery()
        {
            return Sql.Query.Sqlite<JoinedQueryClass>()
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
        public async Task OrderBy()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .OrderBy(x => x.ThePerson.Id)
                .Map(q => q.ThePerson.Name)
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.John.Name, Data.People.Mary.Name }, data);
        }

        [Test]
        public async Task OrderByDescending()
        {
            // ##############################################

            // arrange
            // act
            var data = await FullyJoinedQuery()
                .OrderByDesc(x => x.ThePerson.Id)
                .Map(q => q.ThePerson.Name)
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.Mary.Name, Data.People.John.Name }, data);
        }

        [Test]
        public async Task OrderBy_ThenBy()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .OrderBy(x => x.ThePerson.Id)
                .ThenBy(x => x.TheClasses.One().Id)
                .Map(q => new 
                {
                    name = q.ThePerson.Name,
                    classes = q.TheClasses
                        .Select(c => c.Name)
                })
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.John.Name, Data.People.Mary.Name }, data.Select(x => x.name));
            CollectionAssert.AreEqual(new[] { Data.Classes.Tennis.Name, Data.Classes.Archery.Name, Data.Classes.Tennis.Name }, data.SelectMany(x => x.classes));
        }

        [Test]
        public async Task OrderByDesc_ThenBy()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .OrderByDesc(x => x.ThePerson.Id)
                .ThenBy(x => x.TheClasses.One().Id)
                .Map(q => new 
                {
                    name = q.ThePerson.Name,
                    classes = q.TheClasses
                        .Select(c => c.Name)
                })
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.Mary.Name, Data.People.John.Name }, data.Select(x => x.name));
            CollectionAssert.AreEqual(new[] { Data.Classes.Tennis.Name, Data.Classes.Tennis.Name, Data.Classes.Archery.Name }, data.SelectMany(x => x.classes));
        }

        [Test]
        public async Task OrderByDesc_ThenByDesc()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .OrderByDesc(x => x.ThePerson.Id)
                .ThenByDesc(x => x.TheClasses.One().Id)
                .Map(q => new 
                {
                    name = q.ThePerson.Name,
                    classes = q.TheClasses
                        .Select(c => c.Name)
                })
                .ToArrayAsync(Executor, logger: Logger);
                
            // assert
            CollectionAssert.AreEqual(new[] { Data.People.Mary.Name, Data.People.John.Name }, data.Select(x => x.name));
            CollectionAssert.AreEqual(new[] { Data.Classes.Tennis.Name, Data.Classes.Archery.Name, Data.Classes.Tennis.Name }, data.SelectMany(x => x.classes));
        }

        [Test]
        public async Task OrderBy_ThenByDesc()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .OrderBy(x => x.ThePerson.Id)
                .ThenByDesc(x => x.TheClasses.Select(y => y.Id))
                .Map(q => new 
                {
                    name = q.ThePerson.Name,
                    classes = q.TheClasses
                        .Select(c => c.Name)
                })
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.John.Name, Data.People.Mary.Name }, data.Select(x => x.name));
            CollectionAssert.AreEqual(
                new[] { Data.Classes.Archery.Name, Data.Classes.Tennis.Name, Data.Classes.Tennis.Name }, 
                data.SelectMany(x => x.classes));
        }

        [Test]
        public async Task OrderBy_WithBinaryCondition()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<PersonClass>()
                .From()
                .OrderByDesc(x => x.PersonId + x.ClassId)
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] 
            { 
                Data.PersonClasses.JohnArchery,
                Data.PersonClasses.MaryTennis,
                Data.PersonClasses.JohnTennis
            }, data);
        }

        [Test]
        public async Task OrderBy_WithArgsCondition()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<int, PersonClass>()
                .From()
                .OrderByDesc((x, a) => a)
                .ToArrayAsync(Executor, 7, logger: Logger);

            // assert
            Assert.AreEqual(3, data.Length);
        }
    }
}
