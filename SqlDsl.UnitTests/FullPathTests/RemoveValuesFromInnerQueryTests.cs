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
        public async Task OrderByTableNotInSelect()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .OrderBy(x => x.TheTags.One().Id)
                .Map(q => q.ThePerson.Name)
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.John.Name, Data.People.Mary.Name }, data);
        }

        [Test]
        public async Task WhereTableNotInSelect()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Where(x => x.TheClasses.One().Id == Data.Classes.Archery.Id)
                .Map(q => q.ThePerson.Name)
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new[] { Data.People.John.Name }, data);
        }
    }
}
