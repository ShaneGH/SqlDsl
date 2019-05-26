using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;
using SqlDsl.Sqlite;
using SqlDsl.UnitTests.FullPathTests.Environment;
using SqlDsl.UnitTests.Utils;

namespace SqlDsl.UnitTests.FullPathTests
{
    public class FullPathTestBase
    {
        public readonly TestFlavour TestFlavour;

        public FullPathTestBase(TestFlavour testFlavour)
        {
            TestFlavour = testFlavour;
        }

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

        public Dsl.ISqlSelect<T> Query<T>(bool strictJoins = true)
        {
            return TestUtils.Query<T>(TestFlavour, strictJoins);
        }

        public Dsl.ISqlSelect<TArgs, T> Query<TArgs, T>(bool strictJoins = true)
        {
            return TestUtils.Query<TArgs, T>(TestFlavour, strictJoins);
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
    
    public enum TestFlavour
    {
        Sqlite
    }

    [System.AttributeUsage(System.AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
    sealed class SqlTestAttribute : TestFixtureAttribute, IFixtureBuilder
    {        
        private readonly TestFlavour Language;
        public SqlTestAttribute(TestFlavour language)
            : base(language)
        {
            Language = language;
        }

        IEnumerable<TestSuite> IFixtureBuilder.BuildFrom(ITypeInfo typeInfo)
        {
            switch (Language)
            {
                case TestFlavour.Sqlite:
                    return Run(TestSettings.Instance.Environments.Sqlite);

                default:
                    throw new Exception($"Invalid sql type {Language}");
            }

            IEnumerable<TestSuite> Run(bool run)
            {
                return run
                    ? base.BuildFrom(typeInfo)
                    : new []
                    {
                        new TestSuite($"Test {typeInfo.Name} disabled for {Language}")
                    };
            }
        }
    }
}