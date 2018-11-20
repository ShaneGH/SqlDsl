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
        public async Task WarningWhenCollectionTypeIsWrong()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            await FullyJoinedQuery()
                .Map(p => p.PersonsData.Data.ToList())
                .ToListAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.Contains(Logger.WarningMessages, @"[SqlDsl, 30000] Converting Byte[] to type System.Collections.Generic.List`1[System.Byte]. This conversion is inefficient. Consider changing the result data type to Byte[]");    
        }

        [Test]
        public async Task LogsObjectAllocations()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            await FullyJoinedQuery()
                .Map(p => new { data = p.PersonsData.Data.ToList() })
                .ToListAsync(Executor, logger: Logger);

            PrintAllLogs();

            // assert
            CollectionAssert.Contains(Logger.DebugMessages, "[SqlDsl, 10000] Object graph created");    
        }

        [Test]
        public async Task LogsSqlQueryAsync()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            await Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .ToListAsync(Executor, logger: Logger);

            PrintAllLogs();

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"SELECT [ThePerson].[##rowid] AS [ThePerson.##rowid],[ThePerson].[Id] AS [ThePerson.Id],[ThePerson].[Name] AS [ThePerson.Name],[ThePerson].[Gender] AS [ThePerson.Gender]") &&
                m.Contains("[SqlDsl, 20000] Executing sql:")).Count();
            Assert.True(sql > 0);
        }

        [Test]
        public void LogsSqlQuery()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .ToList(Executor, logger: Logger);

            PrintAllLogs();

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"SELECT [ThePerson].[##rowid] AS [ThePerson.##rowid],[ThePerson].[Id] AS [ThePerson.Id],[ThePerson].[Name] AS [ThePerson.Name],[ThePerson].[Gender] AS [ThePerson.Gender]") &&
                m.Contains("[SqlDsl, 20000] Executing sql:")).Count();
            Assert.True(sql > 0);
        }

        [Test]
        public async Task LogsSqlTimeAsync()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            await Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .ToListAsync(Executor, logger: Logger);

            PrintAllLogs();

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"[SqlDsl, 20001] Executed sql in")).Count();
            Assert.True(sql > 0);
        }

        [Test]
        public void LogsSqlTimeQuery()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .ToList(Executor, logger: Logger);

            PrintAllLogs();

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"[SqlDsl, 20001] Executed sql in")).Count();
            Assert.True(sql > 0);
        }

        void PrintAllLogs()
        {
            Console.WriteLine("DEBUG:");
            Logger.DebugMessages.ForEach(Console.WriteLine);

            Console.WriteLine();
            Console.WriteLine("INFO:");
            Logger.InfoMessages.ForEach(Console.WriteLine);

            Console.WriteLine();
            Console.WriteLine("WARNING");
            Logger.WarningMessages.ForEach(Console.WriteLine);
        }
    }
}
