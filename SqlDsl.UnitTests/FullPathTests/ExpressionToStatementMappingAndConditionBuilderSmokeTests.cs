using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.Dsl;
using SqlDsl.UnitTests.FullPathTests.Environment;
using SqlDsl.Utils;

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class ExpressionToStatementMappingAndConditionBuilderSmokeTests : FullPathTestBase
    {
        public class QueryClass
        {
            public Person ThePerson;
            public IEnumerable<TableWithOneRowAndOneColumn> Other;
        }

        public static IEnumerable<TestCaseData> GetConditionAndMappingParts()
        {
            yield return TC("constant", x => true, 2);
            yield return TC("convert", x => x.ThePerson.Id == (long)1);
            yield return TC("and", x => x.ThePerson.Id == 1 && x.ThePerson.Id == 1);
            yield return TC("or", x => x.ThePerson.Id == 1 || x.ThePerson.Id == 1);
            yield return TC("add", x => x.ThePerson.Id + 1 == 2);
            yield return TC("subtract", x => x.ThePerson.Id - 1 == 0);
            yield return TC("multiply", x => x.ThePerson.Id * 5 == 5);
            yield return TC("divide", x => (x.ThePerson.Id * 10) / 5 == 2);
            yield return TC("equality", x => x.ThePerson.Id == 1);
            yield return TC("non equality", x => x.ThePerson.Id != 2);
            yield return TC("greater than", x => x.ThePerson.Id > 1);
            yield return TC("greater than equal", x => x.ThePerson.Id >= 2);
            yield return TC("less than", x => x.ThePerson.Id < 2);
            yield return TC("less than equal", x => x.ThePerson.Id <= 1);
            yield return TC("member access", x => x.ThePerson.Id == 1);
            yield return TC("new array init", x => x.ThePerson.Id.In(new [] { 1L }));
            yield return TC("new array bounds", x => x.ThePerson.Id.In(new long[1] { 1 }));
            yield return TC("list init", x => x.ThePerson.Id.In(new List<long> { 1L }));

            var inData = new [] { 1L, 100L };
            yield return TC("in 1", x => x.ThePerson.Id.In(new [] { 1L, 100L }));
            yield return TC("in 2", x => x.ThePerson.Id.In(inData));
            yield return TC("in 3", x => inData.Contains(x.ThePerson.Id));
            yield return TC("in 4", x => new [] { 1L, 100L }.Contains(x.ThePerson.Id));
            yield return TC("constant", x => x.ThePerson.Id == 1 && true);
            yield return TC("row number", x => x.RowNumber() == 1);
            yield return TC("One", x => x.Other.One().Value != -99, 2);
            yield return TC("Single", x => x.Other.Single().Value != -99, 2);
            yield return TC("ToArray", x => x.Other.ToArray().One().Value != -99, 2);
            yield return TC("ToList", x => x.Other.ToList().One().Value != -99, 2);

            TestCaseData TC(string name, Expression<Func<QueryClass, bool>> statement, int expectedElements = 1) =>
                new TestCaseData(statement, expectedElements).SetName(name);
        }

        [Test]
        [TestCaseSource("GetConditionAndMappingParts")]
        public void ConditionAndMappingParts(Expression<Func<QueryClass, bool>> condition, int expectedElements)
        {
            // arrange
            var joinCondition = Expression
                .Lambda<Func<QueryClass, TableWithOneRowAndOneColumn, bool>>(
                    condition.Body,
                    condition.Parameters.Append(Expression
                        .Parameter(typeof(TableWithOneRowAndOneColumn))));

            // act
            var result = Sql.Query
                .Sqlite<QueryClass>()
                .From(x => x.ThePerson)
                .InnerJoin(x => x.Other)
                .On(joinCondition)
                .Where(condition)
                .OrderBy(condition)
                .Map(condition)
                .ToList(Executor, logger: Logger);

            // assert
            Assert.AreEqual(expectedElements, result.Count);
            Assert.True(result.All(x => x));
        }

        class NewWithProps : EqComparer
        {
            public int v;

            public override int GetHashCode() => v.GetHashCode();

            public override bool Equals(object o) => o is NewWithProps && (o as NewWithProps).v == v;
        }

        public static IEnumerable<TestCaseData> GetMappingOnlyParts()
        {
            yield return TC("count (length)", x => x.Other.ToArray().Length, 1);
            yield return TC("count (Count())", x => x.Other.Count(), 1);
            yield return TC("count (Count)", x => x.Other.ToList().Count, 1);
            yield return TC("max 1", x => x.Other.Select(o => o.Value).Max(), Data.TablesWithOneRowAndOneColumn.Record.Value);
            yield return TC("max 2", x => x.Other.Max(o => o.Value), Data.TablesWithOneRowAndOneColumn.Record.Value);
            yield return TC("min 1", x => x.Other.Select(o => o.Value).Min(), Data.TablesWithOneRowAndOneColumn.Record.Value);
            yield return TC("min 2", x => x.Other.Min(o => o.Value), Data.TablesWithOneRowAndOneColumn.Record.Value);
            yield return TC("sum 1", x => x.Other.Select(o => o.Value).Sum(), Data.TablesWithOneRowAndOneColumn.Record.Value);
            yield return TC("sum 2", x => x.Other.Sum(o => o.Value), Data.TablesWithOneRowAndOneColumn.Record.Value);
            yield return TC("avg 1", x => x.Other.Select(o => o.Value).Average(), Data.TablesWithOneRowAndOneColumn.Record.Value);
            yield return TC("avg 2", x => x.Other.Average(o => o.Value), Data.TablesWithOneRowAndOneColumn.Record.Value);
            yield return TC("Select(...)", x => x.Other.Select(o => o.Value).One(), Data.TablesWithOneRowAndOneColumn.Record.Value);
            yield return TC("New, with args", 
                x => x.Other.Select(o => new { v = o.Value }).One(), 
                new { v = Data.TablesWithOneRowAndOneColumn.Record.Value });
            yield return TC("New, with props", 
                x => x.Other.Select(o => new NewWithProps { v = o.Value }).One(), 
                new NewWithProps { v = Data.TablesWithOneRowAndOneColumn.Record.Value });

            TestCaseData TC<T>(string name, Expression<Func<QueryClass, T>> statement, T result) =>
                new TestCaseData(statement, result, typeof(T)).SetName(name);
        }

        static IQuery<QueryClass> JoinedQuery() => Sql.Query
            .Sqlite<QueryClass>()
            .From(x => x.ThePerson)
            .InnerJoin(x => x.Other)
            .On((q, x) => true);

        [Test]
        [TestCaseSource("GetMappingOnlyParts")]
        public void MappingOnlyParts(object mapper, object result, Type resultType)
        {
            ReflectionUtils.GetMethod(() =>
                MappingOnlyParts_Executor<object>(null, null), resultType)
                .Invoke(this, new[] { mapper, result });
        }

        [Test]
        public void PreservesOperatorPrecedence()
        {
            // arrange
            // act
            var data = Sql.Query.Sqlite<Person>()
                .Map(p => p.Id + 1 == Data.People.John.Id + 1)
                .ToList(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] {true, false}, data);
        }

        [Test]
        public async Task WorksInsideComplexMap()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => new
                {
                    pid = p.ThePerson.Id + 1,
                    classes = p.ThePersonClasses
                        .Select(pc => pc.ClassId + p.TheClasses.One().Id)
                        .ToList()
                })
                .ToArrayAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Id + 1, data[0].pid);
            CollectionAssert.AreEqual(new [] {Data.Classes.Tennis.Id * 2, Data.Classes.Archery.Id * 2}, data[0].classes);

            Assert.AreEqual(Data.People.Mary.Id + 1, data[1].pid);
            CollectionAssert.AreEqual(new [] {Data.Classes.Tennis.Id * 2}, data[1].classes);
        }

        [Test]
        public async Task WorksAcrossContextBounds()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => new
                {
                    name = p.ThePerson.Name,
                    classes= p.TheClasses.Select(c => c.Id + p.ThePerson.Id)
                })
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count);
            Assert.AreEqual(Data.People.John.Name, data[0].name);
            CollectionAssert.AreEqual(new[] { 4, 5 }, data[0].classes);
            
            Assert.AreEqual(Data.People.Mary.Name, data[1].name);
            CollectionAssert.AreEqual(new[] { 5 }, data[1].classes);
        }

        void MappingOnlyParts_Executor<T>(Expression<Func<QueryClass, T>> mapper, T result)
        {
            // arrange
            // act
            var data = JoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(mapper)
                .ToList(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count);
            Assert.AreEqual(result, data[0]);
        }
    }
}