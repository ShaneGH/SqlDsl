using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(SqlType.MySql)]
    public class LogMessageTests : FullPathTestBase
    {
        public LogMessageTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
        [Test]
        public async Task WarningWhenCollectionTypeIsWrong()
        {
            // arrange
            PrintStatusOnFailure = false;

            // act
            await TestUtils.FullyJoinedQuery(SqlType)
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
            await TestUtils.FullyJoinedQuery(SqlType)
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
            await Query<QueryContainer>()
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
            Query<QueryContainer>()
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
            await Query<QueryContainer>()
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
            Query<QueryContainer>()
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
            TestUtils.FullyJoinedQuery(SqlType)
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
            TestUtils.FullyJoinedQuery(SqlType)
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
            TestUtils.FullyJoinedQuery(SqlType)
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
            TestUtils.FullyJoinedQuery(SqlType)
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
            await TestUtils.FullyJoinedQuery(SqlType)
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
            TestUtils.FullyJoinedQuery(SqlType)
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
            await TestUtils.FullyJoinedQuery(SqlType)
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
