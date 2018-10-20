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
    class TestExecutor : IDebugExecutor
    {
        public readonly IExecutor Executor;
        readonly List<(string sql, string[] colNames, List<object[]> results)> SqlStatements = new List<(string, string[], List<object[]>)>();

        public TestExecutor(IExecutor executor)
        {
            Executor = executor;
        }

        public async Task<IReader> ExecuteAsync(string sql, IEnumerable<object> paramaters, string[] colNames)
        {
            AddSqlStatement(sql, paramaters, colNames);
            return new TestReader(this, await Executor.ExecuteAsync(sql, paramaters), SqlStatements.Count - 1);
        }

        public Task<IReader> ExecuteAsync(string sql, IEnumerable<object> paramaters)
        {
            throw new NotImplementedException("Code should use overload with colNames");
        }

        void AddSqlStatement(string sql, IEnumerable<object> paramaters, string[] colNames)
        {
            SqlStatements.Add((paramaters
                .OrEmpty()
                .Select((p, i) => $"@p{i} = {p}")
                .JoinString("\n") + "\n\n" +
                sql, colNames, new List<object[]>()));
        }

        string SqlStatementString((string sql, string[] colNames, List<object[]> results) statement)
        {
            var results = "[\n" + statement.results
                .Select(row => "  {\n" + row
                    .Select((cell, i) => $"    {statement.colNames[i]}: {cell}")
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

        public IReader Execute(string sql, IEnumerable<object> paramaters, string[] columnNames)
        {
            AddSqlStatement(sql, paramaters, columnNames);
            return new TestReader(this, Executor.Execute(sql, paramaters), SqlStatements.Count - 1);
        }

        public IReader Execute(string sql, IEnumerable<object> paramaters)
        {
            throw new NotImplementedException("Use async method with columnNames");
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

        public (bool hasRow, object[] row) GetRow()
        {
            var row = Reader.GetRow();
            if (row.hasRow)
                Executor.RecordRow(Index, row.row);

            return row;
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
