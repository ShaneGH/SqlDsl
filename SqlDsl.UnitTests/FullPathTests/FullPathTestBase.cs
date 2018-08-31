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
        
        internal TestExecutor Executor;

        [SetUp]
        public void SetUp()
        {
            Executor = new TestExecutor(new SqliteExecutor(Connection));
        }

        [TearDown]
        public void TearDown()
        {
            if (TestContext.CurrentContext.Result.Outcome.Status == TestStatus.Failed)
            {
                Executor.PrintSqlStatements();
            }
        }
    }
}