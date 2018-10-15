// using NUnit.Framework;
// using SqlDsl.Dsl;
// using SqlDsl.Query;
// using SqlDsl.SqlBuilders;
// using SqlDsl.Utils;
// using System;
// using System.Collections.Generic;
// using System.Linq;

// namespace SqlDsl.UnitTests.SqlFlavours
// {
//     public abstract class SqlFragmentBuilderTestBase<TSqlBuilder>
//         where TSqlBuilder : ISqlFragmentBuilder, new()
//     {
//         const string TestDataTableName = "TestDataTable";

//         [OneTimeSetUp]
//         public virtual void FixtureSetup()
//         {
//             CreateDb(BuildDescriptor(TestDataTableName, typeof(TestDataTable)));
//             SeedDb(TestDataTableName, GetRows(TestDataTables.DataTypeTestNotNulled));
//         }

//         [OneTimeTearDown]
//         public virtual void FixtureTeardown()
//         {
//             DropDb();
//         }

//         public abstract void CreateDb(TableDescriptor table);
//         public abstract void SeedDb(string tableName, IEnumerable<IEnumerable<KeyValuePair<string, object>>> rows);
//         public abstract void DropDb();
//         public abstract IExecutor GetExecutor();

//         class One2One
//         {
//             public TestDataTable T1;
//             public TestDataTable T2;
//         }

//         [Test]
//         public void TestJoins()
//         {
//             // arrange
//             // act
//             var values = ((ITable<One2One>)new QueryBuilder<TSqlBuilder, One2One>())
//                 .From(TestDataTableName, x => x.T1)
//                 // .InnerJoin(TestDataTableName, x => x.T2)
//                 //     .On((q, t2) => q.T1.Int == t2.Int)
//                 .Execute(GetExecutor())
//                 .ToList();

//             // assert
//             // Assert.AreEqual(2, values.Count);

//             // Assert.AreEqual(TestDataTables.DataTypeTestNotNulled.Int, values[0].T1.Int);
//             // Assert.AreEqual(TestDataTables.DataTypeTestNotNulled.Int, values[0].T2.Int);

//             // Assert.AreEqual(TestDataTables.DataTypeTestNulled.Int, values[1].T1.Int);
//             // Assert.AreEqual(TestDataTables.DataTypeTestNulled.Int, values[1].T2.Int);
//         }

//         static IEnumerable<IEnumerable<KeyValuePair<string, object>>> GetRows<T>(params T[] dataRows)
//         {
//             var result = new List<IEnumerable<KeyValuePair<string, object>>>();
//             foreach (var dataRow in dataRows)
//             {
//                 var row = new List<KeyValuePair<string, object>>();
//                 foreach (var prop in typeof(T).GetProperties())
//                 {
//                     row.Add(new KeyValuePair<string, object>(prop.Name, prop.GetMethod.Invoke(dataRow, new object[0])));
//                 }
                
//                 foreach (var field in typeof(T).GetFields())
//                 {
//                     row.Add(new KeyValuePair<string, object>(field.Name, field.GetValue(dataRow)));
//                 }

//                 result.Add(row.OrderBy(r => r.Key));
//             }

//             return result;
//         }

//         static TableDescriptor BuildDescriptor(string name, Type t)
//         {
//             return new TableDescriptor
//             {
//                 Name = name,
//                 Columns = ReflectionUtils
//                     .GetFieldsAndProperties(t)
//                     .OrderBy(x => x.name)
//                     .Select(GetColumn)
//                     .Enumerate()
//             };
//         }

//         static ColumnDescriptor GetColumn((string name, Type type) coll)
//         {
//             return new ColumnDescriptor
//             {
//                 Name = coll.name,
//                 Nullable = coll.type.IsClass || coll.type.IsInterface || coll.type.FullName.StartsWith("System.Nullable`1[["),
//                 DataType = GetDataType(coll.type)
//             };
//         }

//         static Type GetDataType(Type type)
//         {
//             type = type.FullName.StartsWith("System.Nullable`1[[") ?
//                 type.GetGenericArguments()[0] :
//                 type;

//             return type.IsEnum ? typeof(int) : type;
//         }
//     }
// }