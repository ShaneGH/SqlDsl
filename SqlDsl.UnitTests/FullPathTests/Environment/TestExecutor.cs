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
        readonly List<(string sql, List<object[]> results)> SqlStatements = new List<(string sql, List<object[]> results)>();

        public TestExecutor(IExecutor executor)
        {
            Executor = executor;
        }

        public async Task<IReader> ExecuteAsync(string sql, IEnumerable<object> paramaters)
        {
            AddSqlStatement(sql, paramaters);
            return new TestReader(this, await Executor.ExecuteAsync(sql, paramaters), SqlStatements.Count - 1);
        }

        void AddSqlStatement(string sql, IEnumerable<object> paramaters)
        {
            SqlStatements.Add((paramaters
                .OrEmpty()
                .Select((p, i) => $"@p{i} = {p}")
                .JoinString("\n") + "\n\n" +
                sql, new List<object[]>()));
        }

        string SqlStatementString((string sql, List<object[]> results) statement)
        {
            var results = "[\n" + statement.results
                .Select(row => "  {\n" + row
                    .Select((cell, i) => $"    {i}: {cell}")
                    .JoinString(",\n") + 
                "\n  }")
                .JoinString(",\n") + "\n]\n";

            return statement.sql + "\n\nResults:\n" + results;
        }

        public void PrintSqlStatements() => Console.WriteLine("\n" + GetSqlStatements());

        public string GetSqlStatements() => $"{SqlStatements.Count} SQL statement(s):\n" + SqlStatements.Select(SqlStatementString).JoinString("\n\n");

        public void RecordRow(int index, object[] row)
        {
            SqlStatements[index].results.Add(row);
        }
    }

    class TestReader : IReader
    {
        TestExecutor Executor;
        IReader Reader;
        int Index;

        public TestReader(TestExecutor executor, IReader reader, int index)
        {
            Executor = executor;
            Reader = reader;
            Index = index;
        }

        public async Task<(bool hasRow, object[] row)> GetRowAsync()
        {
            var row = await Reader.GetRowAsync();
            if (row.hasRow)
                Executor.RecordRow(Index, row.row);

            return row;
        }
    }
}
