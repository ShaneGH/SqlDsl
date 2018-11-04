using NUnit.Framework;
using NUnit.Framework.Interfaces;
using SqlDsl.Dsl;
using SqlDsl.Query;
using SqlDsl.SqlBuilders;
using SqlDsl.UnitTests.FullPathTests.Environment;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SqlDsl.UnitTests.SqlFlavours
{
    public abstract class SqlFragmentBuilderTestBase<TSqlBuilder>
        where TSqlBuilder : ISqlFragmentBuilder, new()
    {
        const string TestDataTableName = "TestDataTable";
        protected bool PrintStatusOnFailure;        
        internal TestExecutor Executor;

        [OneTimeSetUp]
        public virtual void FixtureSetup()
        {
            CreateDb(BuildDescriptor(TestDataTableName, typeof(TestDataTable)));
            SeedDb(TestDataTableName, GetRows(TestDataTables.DataTypeTestNotNulled, TestDataTables.DataTypeTestNulled));
        }

        [OneTimeTearDown]
        public virtual void FixtureTeardown()
        {
            DropDb();
        }

        [SetUp]
        public void SetUp()
        {
            PrintStatusOnFailure = true;
            Executor = new TestExecutor(CreateExecutor());
        }

        [TearDown]
        public void TearDown()
        {
            if (PrintStatusOnFailure && TestContext.CurrentContext.Result.Outcome.Status == TestStatus.Failed)
            {
                Executor.PrintSqlStatements();
            }

            DisposeOfExecutor(Executor.Executor);
        }

        public abstract void DisposeOfExecutor(IExecutor executor);

        public abstract void CreateDb(TableDescriptor table);
        public abstract void SeedDb(string tableName, IEnumerable<IEnumerable<KeyValuePair<string, object>>> rows);
        public abstract void DropDb();
        public abstract IExecutor CreateExecutor();

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
            var values = ((ITable<TestDataTable>)new QueryBuilder<TSqlBuilder, TestDataTable>())
                .From()
                .Execute(Executor)
                .ToList();

            // assert
            Assert.AreEqual(2, values.Count);
            Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
            Compare(TestDataTables.DataTypeTestNulled, values[1]);
        }

        [Test]
        public void TestJoins()
        {
            // arrange
            // act
            var values = ((ITable<One2One>)new QueryBuilder<TSqlBuilder, One2One>())
                .From(x => x.T1)
                .InnerJoin(x => x.T2).On((q, t2) => q.T1.PrimaryKey == t2.PrimaryKey)
                .Execute(Executor)
                .ToList();

            // assert
            Assert.AreEqual(2, values.Count);
            Compare(TestDataTables.DataTypeTestNotNulled, values[0].T1);
            Compare(TestDataTables.DataTypeTestNotNulled, values[0].T2);
            Compare(TestDataTables.DataTypeTestNulled, values[1].T1);
            Compare(TestDataTables.DataTypeTestNulled, values[1].T2);
        }

        [Test]
        public void TestEquality()
        {
            // arrange
            // act
            var values = ((ITable<TestDataTable>)new QueryBuilder<TSqlBuilder, TestDataTable>())
                .From()
                .Where(x => x.PrimaryKey == TestDataTables.DataTypeTestNotNulled.PrimaryKey)
                .Execute(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
        }

        [Test]
        [Ignore("TODO")]
        public void TestEqualityForNullable()
        {
            // arrange
            // act
            var values = ((ITable<TestDataTable>)new QueryBuilder<TSqlBuilder, TestDataTable>())
                .From()
                .Where(x => x.Float_N == null)
                .Execute(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            Compare(TestDataTables.DataTypeTestNulled, values[0]);
        }

        [Test]
        public void TestNonEquality()
        {
            // arrange
            // act
            var values = ((ITable<TestDataTable>)new QueryBuilder<TSqlBuilder, TestDataTable>())
                .From()
                .Where(x => x.PrimaryKey != TestDataTables.DataTypeTestNotNulled.PrimaryKey)
                .Execute(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            Compare(TestDataTables.DataTypeTestNulled, values[0]);
        }

        [Test]
        [Ignore("TODO")]
        public void TestNonEqualityForNullable()
        {
            // arrange
            // act
            var values = ((ITable<TestDataTable>)new QueryBuilder<TSqlBuilder, TestDataTable>())
                .From()
                .Where(x => x.Float_N != null)
                .Execute(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
        }

        [Test]
        public void TestEnumValue()
        {
            // arrange
            // act
            var values = ((ITable<TestDataTable>)new QueryBuilder<TSqlBuilder, TestDataTable>())
                .From()
                .Where(x => x.TestEnum == TestEnum.Option1 && x.PrimaryKey == TestDataTables.DataTypeTestNotNulled.PrimaryKey)
                .Execute(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
        }

        [Test]
        public void TestAndCondition()
        {
            // arrange
            // act
            var values = ((ITable<TestDataTable>)new QueryBuilder<TSqlBuilder, TestDataTable>())
                .From()
                .Where(x => x.PrimaryKey == TestDataTables.DataTypeTestNotNulled.PrimaryKey && x.DateTime == TestDataTables.DataTypeTestNotNulled.DateTime)
                .Execute(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
        }

        [Test]
        public void TestOrCondition()
        {
            // arrange
            // act
            var values = ((ITable<TestDataTable>)new QueryBuilder<TSqlBuilder, TestDataTable>())
                .From()
                .Where(x => x.PrimaryKey == TestDataTables.DataTypeTestNotNulled.PrimaryKey || x.PrimaryKey == 33333)
                .Execute(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
        }

        [Test]
        public void TestLessThan()
        {
            // arrange
            var pk = TestDataTables.DataTypeTestNotNulled.PrimaryKey + 1;

            // act
            var values = ((ITable<TestDataTable>)new QueryBuilder<TSqlBuilder, TestDataTable>())
                .From()
                .Where(x => x.PrimaryKey < pk)
                .Execute(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
        }

        [Test]
        public void TestLessThanEqualTo()
        {
            // arrange
            // act
            var values = ((ITable<TestDataTable>)new QueryBuilder<TSqlBuilder, TestDataTable>())
                .From()
                .Where(x => x.PrimaryKey <= TestDataTables.DataTypeTestNotNulled.PrimaryKey)
                .Execute(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
        }

        [Test]
        public void TestGreaterThan()
        {
            // arrange
            var pk = TestDataTables.DataTypeTestNulled.PrimaryKey - 1;

            // act
            var values = ((ITable<TestDataTable>)new QueryBuilder<TSqlBuilder, TestDataTable>())
                .From()
                .Where(x => x.PrimaryKey > pk)
                .Execute(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            Compare(TestDataTables.DataTypeTestNulled, values[0]);
        }

        [Test]
        public void TestGreaterThanEqualTo()
        {
            // arrange
            // act
            var values = ((ITable<TestDataTable>)new QueryBuilder<TSqlBuilder, TestDataTable>())
                .From()
                .Where(x => x.PrimaryKey >= TestDataTables.DataTypeTestNulled.PrimaryKey)
                .Execute(Executor)
                .ToList();

            // assert
            Assert.AreEqual(1, values.Count);
            Compare(TestDataTables.DataTypeTestNulled, values[0]);
        }

        static IEnumerable<IEnumerable<KeyValuePair<string, object>>> GetRows<T>(params T[] dataRows)
        {
            var result = new List<IEnumerable<KeyValuePair<string, object>>>();
            foreach (var dataRow in dataRows)
            {
                var row = new List<KeyValuePair<string, object>>();
                foreach (var prop in typeof(T).GetProperties())
                {
                    row.Add(new KeyValuePair<string, object>(prop.Name, prop.GetMethod.Invoke(dataRow, new object[0])));
                }
                
                foreach (var field in typeof(T).GetFields())
                {
                    row.Add(new KeyValuePair<string, object>(field.Name, field.GetValue(dataRow)));
                }

                result.Add(row.OrderBy(r => r.Key));
            }

            return result;
        }

        static TableDescriptor BuildDescriptor(string name, Type t)
        {
            return new TableDescriptor
            {
                Name = name,
                Columns = ReflectionUtils
                    .GetFieldsAndProperties(t)
                    .OrderBy(x => x.name)
                    .Select(GetColumn)
                    .Enumerate()
            };
        }

        static ColumnDescriptor GetColumn((string name, Type type, bool isReadOnly) coll)
        {
            switch (coll.name)
            {
                case "NullableValueTypeAsObject":
                    coll = (coll.name, typeof(int?), coll.isReadOnly);
                    break;
                case "ReferenceTypeAsObject":
                    coll = (coll.name, typeof(string), coll.isReadOnly);
                    break;
            }

            return new ColumnDescriptor
            {
                Name = coll.name,
                Nullable = coll.type.IsClass || coll.type.IsInterface || coll.type.FullName.StartsWith("System.Nullable`1[["),
                DataType = GetDataType(coll.type)
            };
        }

        static Type GetDataType(Type type)
        {
            type = type.FullName.StartsWith("System.Nullable`1[[") ?
                type.GetGenericArguments()[0] :
                type;

            return type.IsEnum ? typeof(int) : type;
        }

        static void Compare(TestDataTable expected, TestDataTable actual)
        {
            Assert.AreEqual(expected.PrimaryKey, actual.PrimaryKey);
            Assert.AreEqual(expected.Bool, actual.Bool);
            Assert.AreEqual(expected.Bool_N, actual.Bool_N);
            Assert.AreEqual(expected.Byte, actual.Byte);
            Assert.AreEqual(expected.Byte_N, actual.Byte_N);
            Assert.AreEqual(expected.ByteArray, actual.ByteArray);
            Assert.AreEqual(expected.ByteEnumerable, actual.ByteEnumerable);
            Assert.AreEqual(expected.ByteList, actual.ByteList);
            Assert.AreEqual(expected.Char, actual.Char);
            Assert.AreEqual(expected.Char_N, actual.Char_N);
            Assert.AreEqual(expected.CharArray, actual.CharArray);
            Assert.AreEqual(expected.CharEnumerable, actual.CharEnumerable);
            Assert.AreEqual(expected.CharList, actual.CharList);
            Assert.AreEqual(expected.DateTime, actual.DateTime);
            Assert.AreEqual(expected.DateTime_N, actual.DateTime_N);
            Assert.AreEqual(expected.Decimal, actual.Decimal);
            Assert.AreEqual(expected.Decimal_N, actual.Decimal_N);
            Assert.AreEqual(expected.Double, actual.Double);
            Assert.AreEqual(expected.Double_N, actual.Double_N);
            Assert.AreEqual(expected.Float, actual.Float);
            Assert.AreEqual(expected.Float_N, actual.Float_N);
            Assert.AreEqual(expected.Guid, actual.Guid);
            Assert.AreEqual(expected.Guid_N, actual.Guid_N);
            Assert.AreEqual(expected.Int, actual.Int);
            Assert.AreEqual(expected.Int_N, actual.Int_N);
            Assert.AreEqual(expected.Long, actual.Long);
            Assert.AreEqual(expected.Long_N, actual.Long_N);
            Assert.AreEqual(expected.SByte, actual.SByte);
            Assert.AreEqual(expected.SByte_N, actual.SByte_N);
            Assert.AreEqual(expected.Short, actual.Short);
            Assert.AreEqual(expected.Short_N, actual.Short_N);
            Assert.AreEqual(expected.String, actual.String);
            Assert.AreEqual(expected.TestEnum, actual.TestEnum);
            Assert.AreEqual(expected.TestEnum_N, actual.TestEnum_N);
            Assert.AreEqual(expected.UInt, actual.UInt);
            Assert.AreEqual(expected.UInt_N, actual.UInt_N);
            Assert.AreEqual(expected.ULong, actual.ULong);
            Assert.AreEqual(expected.ULong_N, actual.ULong_N);
            Assert.AreEqual(expected.UShort, actual.UShort);
            Assert.AreEqual(expected.UShort_N, actual.UShort_N);
            Assert.AreEqual(expected.ReferenceTypeAsObject, actual.ReferenceTypeAsObject);
            Assert.AreEqual(expected.NullableValueTypeAsObject, actual.NullableValueTypeAsObject);
        }
    }
}