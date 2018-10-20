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
    public class LogTests : FullPathTestBase
    {
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public PersonsData PersonsData { get; set; }
            public List<PersonClass> PersonClasses { get; set; }
            public List<Class> Classes { get; set; }
            public List<ClassTag> ClassTags { get; set; }
            public List<Tag> Tags { get; set; }
        }

        static Dsl.IQuery<JoinedQueryClass> FullyJoinedQuery()
        {
            return Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<PersonsData>(q => q.PersonsData)
                    .On((q, pd) => q.ThePerson.Id == pd.PersonId)
                .InnerJoin<Class>(q => q.Classes)
                    .On((q, c) => q.PersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.ClassTags)
                    .On((q, ct) => q.Classes.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.Tags)
                    .On((q, t) => q.ClassTags.One().TagId == t.Id);
        }

        [Test]
        [Ignore("TODO")]
        public async Task WarningWhenCollectionTypeIsWrong()
        {
            // arrange
            // act
            await FullyJoinedQuery()
                .Map(p => p.PersonsData.Data.ToList())
                .ExecuteAsync(Executor, logger: Logger);

            // assert
            var valsType = "";
            var collectionType = "";
            var propertyName = "";
            CollectionAssert.Contains(Logger.WarningMessages, $"Converting {valsType} to type {collectionType} for property " + 
                    $"\"{propertyName}\". This conversion is inefficient. Consider changing the " + 
                    $"data type of \"{propertyName}\" to {valsType}");    
        }
    }
}
