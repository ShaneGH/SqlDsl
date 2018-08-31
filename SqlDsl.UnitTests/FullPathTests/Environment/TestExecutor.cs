using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using SqlDsl.Utils;

namespace SqlDsl.UnitTests.FullPathTests.Environment
{
    class TestExecutor : IExecutor
    {
        readonly IExecutor Executor;
        readonly List<string> SqlStatements = new List<string>();

        public TestExecutor(IExecutor executor)
        {
            Executor = executor;
        }

        public Task<IReader> ExecuteAsync(string sql, IEnumerable<object> paramaters)
        {
            AddSqlStatement(sql, paramaters);
            return Executor.ExecuteAsync(sql, paramaters);
        }

        void AddSqlStatement(string sql, IEnumerable<object> paramaters)
        {
            SqlStatements.Add(paramaters
                .OrEmpty()
                .Select((p, i) => $"@p{i} = {p}")
                .JoinString("\n") + "\n\n" +
                sql);
        }

        public void PrintSqlStatements() => Console.WriteLine("\n" + GetSqlStatements());

        public string GetSqlStatements() => $"{SqlStatements.Count} SQL statement(s):\n" + SqlStatements.JoinString("\n\n");
    }
}
