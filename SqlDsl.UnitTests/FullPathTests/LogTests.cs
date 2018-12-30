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
            public PersonsData ThePersonsData { get; set; }
            public List<PersonClass> ThePersonClasses { get; set; }
            public List<Class> TheClasses { get; set; }
            public List<ClassTag> TheClassTags { get; set; }
            public List<Tag> TheTags { get; set; }
        }

        static Dsl.IQuery<JoinedQueryClass> FullyJoinedQuery()
        {
            return Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<PersonsData>(q => q.ThePersonsData)
                    .On((q, pd) => q.ThePerson.Id == pd.PersonId)
                .InnerJoin<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id);
        }

        [Test]
        public async Task WarningWhenCollectionTypeIsWrong()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            await FullyJoinedQuery()
                .Map(p => p.ThePersonsData.Data.ToList())
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
                .Map(p => new { data = p.ThePersonsData.Data.ToList() })
                .ToListAsync(Executor, logger: Logger);

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

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"SELECT [ThePerson].[#rowid] AS [ThePerson.#rowid],[ThePerson].[Id] AS [ThePerson.Id],[ThePerson].[Name] AS [ThePerson.Name],[ThePerson].[Gender] AS [ThePerson.Gender]") &&
                m.Contains("[SqlDsl, 20001] Executing sql:")).Count();
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

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"SELECT [ThePerson].[#rowid] AS [ThePerson.#rowid],[ThePerson].[Id] AS [ThePerson.Id],[ThePerson].[Name] AS [ThePerson.Name],[ThePerson].[Gender] AS [ThePerson.Gender]") &&
                m.Contains("[SqlDsl, 20001] Executing sql:")).Count();
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

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"[SqlDsl, 20002] Executed sql in")).Count();
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

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"[SqlDsl, 20002] Executed sql in")).Count();
            Assert.True(sql > 0);
        }

        [Test]
        public void LogsCompileTimeQuery_Unmapped()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            FullyJoinedQuery()
                .ToList(Executor, logger: Logger);

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"[SqlDsl, 20000] Query compiled in")).Count();
            Assert.True(sql > 0);
        }

        [Test]
        public void LogsCompileTimeQuery_SimpleMap()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            FullyJoinedQuery()
                .Map(x => x.ThePerson.Name)
                .ToList(Executor, logger: Logger);

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"[SqlDsl, 20000] Query compiled in")).Count();
            Assert.True(sql > 0);
        }

        [Test]
        public void LogsCompileTimeQuery_ComplexMap()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            FullyJoinedQuery()
                .Map(x => new { val = x.TheClasses })
                .ToList(Executor, logger: Logger);

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"[SqlDsl, 20000] Query compiled in")).Count();
            Assert.True(sql > 0);
        }

        [Test]
        public void LogsParseTime_ToList()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            FullyJoinedQuery()
                .Map(x => new { val = x.TheClasses })
                .ToList(Executor, logger: Logger);

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"[SqlDsl, 20003] Data parsed in")).Count();
            Assert.True(sql > 0);
        }

        [Test]
        public async Task LogsParseTime_ToListAsync()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            await FullyJoinedQuery()
                .Map(x => new { val = x.TheClasses })
                .ToListAsync(Executor, logger: Logger);

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"[SqlDsl, 20003] Data parsed in")).Count();
            Assert.True(sql > 0);
        }

        [Test]
        public void LogsParseTime_ToArray()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            FullyJoinedQuery()
                .Map(x => new { val = x.TheClasses })
                .ToArray(Executor, logger: Logger);

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"[SqlDsl, 20003] Data parsed in")).Count();
            Assert.True(sql > 0);
        }

        [Test]
        public async Task LogsParseTime_ToArrayAsync()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            await FullyJoinedQuery()
                .Map(x => new { val = x.TheClasses })
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            var sql = Logger.InfoMessages.Where(m => 
                m.Contains(@"[SqlDsl, 20003] Data parsed in")).Count();
            Assert.True(sql > 0);
        }

        void PrintAllLogs()
        {
            Logger.PrintAllLogs();
        }
    }
}
