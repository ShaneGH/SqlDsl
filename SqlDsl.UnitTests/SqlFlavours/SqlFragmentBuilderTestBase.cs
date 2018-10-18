using NUnit.Framework;
using SqlDsl.Dsl;
using SqlDsl.Query;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.UnitTests.SqlFlavours
{
    public abstract class SqlFragmentBuilderTestBase<TSqlBuilder>
        where TSqlBuilder : ISqlFragmentBuilder, new()
    {
        const string TestDataTableName = "TestDataTable";

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

        public abstract void CreateDb(TableDescriptor table);
        public abstract void SeedDb(string tableName, IEnumerable<IEnumerable<KeyValuePair<string, object>>> rows);
        public abstract void DropDb();
        public abstract IExecutor GetExecutor();

        class One2One
        {
            public TestDataTable T1;
            public TestDataTable T2;
        }

        [Test]
        public void TestJoins()
        {
            // arrange
            // act
            var values = ((ITable<TestDataTable>)new QueryBuilder<TSqlBuilder, TestDataTable>())
                .From()
                .Execute(GetExecutor())
                .ToList();

            // assert
            Assert.AreEqual(2, values.Count);
            Compare(TestDataTables.DataTypeTestNotNulled, values[0]);
            Compare(TestDataTables.DataTypeTestNulled, values[1]);
        }

        static void Compare(TestDataTable expected, TestDataTable actual)
        {
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

        static ColumnDescriptor GetColumn((string name, Type type) coll)
        {
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
    }
}