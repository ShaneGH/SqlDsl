using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;
using SqlDsl.Dsl;
using SqlDsl.Sqlite;
using SqlDsl.UnitTests.FullPathTests;
using SqlDsl.UnitTests.FullPathTests.Environment;
using SqlDsl.Utils;

namespace SqlDsl
{
    public class Program
    {
        static void Main(string[] args)
        {
            InitData.EnsureInit();
            using (var connection = InitData.CreateConnection())
            {
                connection.Open();
                var Executor = new TestExecutor(new SqliteExecutor(connection));
            }
        }
    }
}
