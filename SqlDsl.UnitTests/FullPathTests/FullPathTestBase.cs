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
        public readonly SqlType SqlType;

        public FullPathTestBase(SqlType testFlavour)
        {
            SqlType = testFlavour;
        }

        SqliteConnection SqliteConnection;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            switch (SqlType)
            {
                case SqlType.Sqlite:
                    InitData.EnsureInit(SqlType);
                    SqliteConnection = InitSqliteDatabase.CreateSqliteConnection();
                    SqliteConnection.Open();
                    break;

                default:
                    throw new Exception($"Invalid sql type {SqlType}");
            }
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            switch (SqlType)
            {
                case SqlType.Sqlite:
                    SqliteConnection.Dispose();
                    break;

                default:
                    throw new Exception($"Invalid sql type {SqlType}");
            }
        }

        protected bool PrintStatusOnFailure;
        internal TestExecutor Executor;
        internal TestLogger Logger;

        [SetUp]
        public void SetUp()
        {
            IExecutor ex;
            switch (SqlType)
            {
                case SqlType.Sqlite:
                    ex = new SqliteExecutor(SqliteConnection);
                    break;

                default:
                    throw new Exception($"Invalid sql type {SqlType}");
            }

            Executor = new TestExecutor(ex);
            PrintStatusOnFailure = true;
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
            return TestUtils.Query<T>(SqlType, strictJoins);
        }

        public Dsl.ISqlSelect<TArgs, T> Query<TArgs, T>(bool strictJoins = true)
        {
            return TestUtils.Query<TArgs, T>(SqlType, strictJoins);
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
    
    public enum SqlType
    {
        Sqlite
    }

    [System.AttributeUsage(System.AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
    sealed class SqlTestAttribute : TestFixtureAttribute, IFixtureBuilder
    {        
        private readonly SqlType Language;
        public SqlTestAttribute(SqlType language)
            : base(language)
        {
            Language = language;
        }

        IEnumerable<TestSuite> IFixtureBuilder.BuildFrom(ITypeInfo typeInfo)
        {
            switch (Language)
            {
                case SqlType.Sqlite:
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