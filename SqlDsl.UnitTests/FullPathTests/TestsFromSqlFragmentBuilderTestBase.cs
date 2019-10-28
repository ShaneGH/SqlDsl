using System.Linq;
using NUnit.Framework;
using SqlDsl.Dsl;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    /// <summary>
    /// These tests were initially in the SqlFragmentBuilderTestBase.
    /// Most of the test cases are redundant, but the asserts are valid, as they
    /// deal with data types
    /// </summary>
    [SqlTestAttribute(SqlType.TSql)]
    [SqlTestAttribute(SqlType.MySql)]
    [SqlTestAttribute(SqlType.Sqlite)]
    public class TestsFromSqlFragmentBuilderTestBase : FullPathTestBase
    {
        public TestsFromSqlFragmentBuilderTestBase(SqlType testFlavour)
            : base(testFlavour)
        {
        }

        ISqlSelect<TestDataTable> StartTest() => Query<TestDataTable>();

        class One2One
        {
            // warning CS0649: Field is never assigned to, and will always have its default value null
            #pragma warning disable 0649
            public TestDataTable T1;
            public TestDataTable T2;
            #pragma warning restore 0649
        }

        [Test]
        public void TestValues()
        {
            // arrange
            // act
            var values = StartTest()
                .ToIEnumerable(Executor)
                .ToList();

            // assert
            Assert.AreEqual(2, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[1]);
        }

        [Test]
        public void TestJoins()
        {
            // arrange
            // act
            var values = Query<One2One>()
                .From(x => x.T1)
                .InnerJoinOne(x => x.T2).On((q, t2) => q.T1.PrimaryKey == t2.PrimaryKey)
                .ToIEnumerable(Executor)
                .ToList();

            // assert
            Assert.AreEqual(2, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNotNulled, values[0].T1);
            TestDataTable.Compare(TestDataTables.DataTypeTestNotNulled, values[0].T2);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[1].T1);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[1].T2);
        }

        [Test]
        public void TestEquality()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.PrimaryKey == TestDataTables.DataTypeTestNotNulled.PrimaryKey)
                .ToIEnumerable(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
        }

        [Test]
        [Ignore("TODO")]
        public void TestEqualityForNullable()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.Float_N == null)
                .ToIEnumerable(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[0]);
        }

        [Test]
        public void TestNonEquality()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.PrimaryKey != TestDataTables.DataTypeTestNotNulled.PrimaryKey)
                .ToIEnumerable(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[0]);
        }

        [Test]
        [Ignore("TODO")]
        public void TestNonEqualityForNullable()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.Float_N != null)
                .ToIEnumerable(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
        }

        [Test]
        public void TestEnumValue()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.TestEnum == TestEnum.Option1 && x.PrimaryKey == TestDataTables.DataTypeTestNotNulled.PrimaryKey)
                .ToIEnumerable(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
        }

        [Test]
        public void TestAndCondition()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.PrimaryKey == TestDataTables.DataTypeTestNotNulled.PrimaryKey && x.DateTime == TestDataTables.DataTypeTestNotNulled.DateTime)
                .ToIEnumerable(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
        }

        [Test]
        public void TestOrCondition()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.PrimaryKey == TestDataTables.DataTypeTestNotNulled.PrimaryKey || x.PrimaryKey == 33333)
                .ToIEnumerable(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
        }

        [Test]
        public void TestLessThan()
        {
            // arrange
            var pk = TestDataTables.DataTypeTestNotNulled.PrimaryKey + 1;

            // act
            var values = StartTest()
                .Where(x => x.PrimaryKey < pk)
                .ToIEnumerable(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
        }

        [Test]
        public void TestLessThanEqualTo()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.PrimaryKey <= TestDataTables.DataTypeTestNotNulled.PrimaryKey)
                .ToIEnumerable(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
        }

        [Test]
        public void TestGreaterThan()
        {
            // arrange
            var pk = TestDataTables.DataTypeTestNulled.PrimaryKey - 1;

            // act
            var values = StartTest()
                .Where(x => x.PrimaryKey > pk)
                .ToIEnumerable(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[0]);
        }

        [Test]
        public void TestGreaterThanEqualTo()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.PrimaryKey >= TestDataTables.DataTypeTestNulled.PrimaryKey)
                .ToList(Executor);

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[0]);
        }

        [Test]
        public void TestIn()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.PrimaryKey.In(new int[1] { TestDataTables.DataTypeTestNulled.PrimaryKey }))
                .ToArray(Executor);

            // assert
            Assert.AreEqual(1, values.Length);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[0]);
        }

        [Test]
        public void TestEmptyIn()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.PrimaryKey.In(new int [0]))
                .ToArray(Executor);

            // assert
            Assert.AreEqual(0, values.Length);
        }

        [Test]
        [Ignore("TODO")]
        public void TestNullIn()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.Byte_N.In(new byte?[] { null }))
                .ToArray(Executor);

            // assert
            Assert.AreEqual(1, values.Length);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[0]);
        }

        [Test]
        public void TestOrderBy()
        {
            // arrange
            // act
            var values = StartTest()
                .OrderBy(x => x.PrimaryKey)
                .ToList(Executor);

            // assert
            Assert.AreEqual(2, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[1]);
        }

        [Test]
        [Ignore("TODO")]
        public void TestOrderByDesc()
        {
            // arrange
            // act
            var values = StartTest()
                .OrderByDesc(x => x.PrimaryKey)
                .ToList(Executor);

            // assert
            Assert.AreEqual(2, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[0]);
            TestDataTable.Compare(TestDataTables.DataTypeTestNotNulled, values[1]);
        }

        [Test]
        public void TestAdd()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.PrimaryKey + 123 == TestDataTables.DataTypeTestNulled.PrimaryKey + 123)
                .ToList(Executor);

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[0]);
        }

        [Test]
        public void TestSubtract()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.PrimaryKey - 123 == TestDataTables.DataTypeTestNulled.PrimaryKey - 123)
                .ToList(Executor);

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[0]);
        }

        [Test]
        public void TestMultiply()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.PrimaryKey * 123 == TestDataTables.DataTypeTestNulled.PrimaryKey * 123)
                .ToList(Executor);

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[0]);
        }

        [Test]
        public void TestDivide()
        {
            // arrange
            // act
            var values = StartTest()
                .Where(x => x.PrimaryKey / 2 == TestDataTables.DataTypeTestNulled.PrimaryKey / 2)
                .ToList(Executor);

            // assert
            Assert.AreEqual(1, values.Count);
            TestDataTable.Compare(TestDataTables.DataTypeTestNulled, values[0]);
        }
    }
}