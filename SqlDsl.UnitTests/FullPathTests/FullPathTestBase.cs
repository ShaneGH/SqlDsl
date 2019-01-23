using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using SqlDsl.Sqlite;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class FullPathTestBase
    {
        SqliteConnection Connection;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            InitData.EnsureInit();
            Connection = InitData.CreateConnection();
            Connection.Open();
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            Connection.Dispose();
        }

        protected bool PrintStatusOnFailure;
        internal TestExecutor Executor;
        internal TestLogger Logger;

        [SetUp]
        public void SetUp()
        {
            PrintStatusOnFailure = true;
            Executor = new TestExecutor(new SqliteExecutor(Connection));
            Logger = new TestLogger();
        }

        [TearDown]
        public void TearDown()
        {
            if (PrintStatusOnFailure && TestContext.CurrentContext.Result.Outcome.Status == TestStatus.Failed)
            {
                Executor.PrintSqlStatements();
            }
        }

        public class TestLogger : ILogger
        {
            public LogLevel LogLevel { get; set; } = LogLevel.Debug;

            public HashSet<LogMessages> SupressLogMessages { get; set; } = new HashSet<LogMessages>();

            public readonly List<string> DebugMessages = new List<string>();
            public readonly List<string> InfoMessages = new List<string>();
            public readonly List<string> WarningMessages = new List<string>();

            public void Debug(string message)
            {
                DebugMessages.Add(message);
            }

            public void Info(string message)
            {
                InfoMessages.Add(message);
            }

            public void Warning(string message)
            {
                WarningMessages.Add(message);
            }

            public void PrintAllLogs()
            {
                Console.WriteLine("DEBUG:");
                DebugMessages.ForEach(Console.WriteLine);

                Console.WriteLine();
                Console.WriteLine("INFO:");
                InfoMessages.ForEach(Console.WriteLine);

                Console.WriteLine();
                Console.WriteLine("WARNING:");
                WarningMessages.ForEach(Console.WriteLine);
            }
        }
    }
}