using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

using SqlDsl.DataParser;
using SqlDsl.SqlBuilders;

namespace SqlDsl.UnitTests.DataParser
{
    [TestFixture]
    public class ParserTests
    {
        static IEnumerable<List<(string column, object value)>> BuildResults(string[] columns, object[][] rows)
        {
            return rows.Select(row => 
                row.Select((cell, i) => (columns.ElementAt(i), cell)).ToList());
        }

        class SimpleProperties 
        {
            public string Col1 { get; set; } 
            public int Col2 { get; set; }

            public SimpleProperties Inner { get; set; }
        }

        [Test]
        public void Parse_WithThreeLevels_ParsesCorrectly()
        {
            // arrange
            var rid = SqlBuilderBase.RowIdName;
            var results = BuildResults(
                new [] {    $"{rid}",   "Col1", "Col2", $"Inner.{rid}", "Inner.Col1",   "Inner.Col2",   $"Inner.Inner.{rid}",   "Inner.Inner.Col1", "Inner.Inner.Col2" },
                new[]
                {
                    new [] { (object)1, "2",    3,      1,              "5",            6,              1,                      "8",                9 },
                    new [] { (object)2, "12",   13,     1,              "14",           15,             2,                      "16",               17 },
                });

            // act
            var parsed = Parser.Parse<SimpleProperties>(results);

            // assert
            Assert.NotNull(parsed);
            Assert.AreEqual(2, parsed.Count());

            var r1 = parsed.First();
            var r2 = parsed.ElementAt(1);

            Assert.AreEqual("2", r1.Col1);
            Assert.AreEqual(3, r1.Col2);
            Assert.AreEqual("5", r1.Inner.Col1);
            Assert.AreEqual(6, r1.Inner.Col2);
            Assert.AreEqual("8", r1.Inner.Inner.Col1);
            Assert.AreEqual(9, r1.Inner.Inner.Col2);            

            Assert.AreEqual("12", r2.Col1);
            Assert.AreEqual(13, r2.Col2);
            Assert.AreEqual("14", r2.Inner.Col1);
            Assert.AreEqual(15, r2.Inner.Col2);
            Assert.AreEqual("16", r2.Inner.Inner.Col1);
            Assert.AreEqual(17, r2.Inner.Inner.Col2);
        }

        class BasicQuery 
        {
            public SimpleProperties T1 { get; set; }
            public IEnumerable<SimpleProperties> T2 { get; set; }
            public IEnumerable<SimpleProperties> T3 { get; set; }
        }

        [Test]
        public void Parse_WithBasicUnmappedQueryResult_ParsesCorrectly()
        {
            // arrange
            var rid = SqlBuilderBase.RowIdName;
            var results = BuildResults(
                new [] {    $"T1.{rid}",    "T1.Col1",  "T1.Col2",  $"T2.{rid}",    "T2.Col1",  "T2.Col2",  $"T3.{rid}",    "T3.Col1",  "T3.Col2" },
                new[]
                {
                    new [] { (object)1,     "2",        3,          1,              "5",        6,          1,              "8",        9 },
                    new [] { (object)1,     "2",        3,          1,              "5",        6,          2,              "10",       11 },
                    new [] { (object)1,     "2",        3,          2,              "12",       13,         1,              "8",        9 },
                    new [] { (object)1,     "2",        3,          2,              "12",       13,         2,              "10",       11 }
                });

            // act
            var parsed = Parser.Parse<BasicQuery>(results, "T1");

            // assert
            Assert.NotNull(parsed);
            Assert.AreEqual(1, parsed.Count());
            Assert.NotNull(parsed.First().T1);
            Assert.AreEqual(2, parsed.First().T2.Count());
            Assert.NotNull(parsed.First().T2.First());
            Assert.AreEqual(2, parsed.First().T3.Count());
            Assert.NotNull(parsed.First().T3.First());

            var t1 = parsed.First().T1;
            var t2_1 = parsed.First().T2.First();
            var t2_2 = parsed.First().T2.ElementAt(1);
            var t3_1 = parsed.First().T3.First();
            var t3_2 = parsed.First().T3.ElementAt(1);

            Assert.AreEqual("2", t1.Col1);
            Assert.AreEqual(3, t1.Col2);

            Assert.AreEqual("5", t2_1.Col1);
            Assert.AreEqual(6, t2_1.Col2);

            Assert.AreEqual("12", t2_2.Col1);
            Assert.AreEqual(13, t2_2.Col2);

            Assert.AreEqual("8", t3_1.Col1);
            Assert.AreEqual(9, t3_1.Col2);

            Assert.AreEqual("10", t3_2.Col1);
            Assert.AreEqual(11, t3_2.Col2);
        }
    }
}