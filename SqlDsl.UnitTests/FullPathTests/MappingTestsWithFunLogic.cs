using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;
using NUnit.Framework;
using SqlDsl.Utils;
using SqlDsl.UnitTests.FullPathTests.Environment;
using SqlDsl.Sqlite;
using NUnit.Framework.Interfaces;
using SqlDsl.ObjectBuilders;
using System.Linq.Expressions;

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class MappingTestsWithFunLogic : FullPathTestBase
    {
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public List<PersonClass> ThePersonClasses { get; set; }
            public List<Class> TheClasses { get; set; }
            public List<ClassTag> TheClassTags { get; set; }
            public List<Tag> TheTags { get; set; }
        }

        static Dsl.IQuery<TArg, JoinedQueryClass> FullyJoinedQuery<TArg>()
        {
            return Sql.Query.Sqlite<TArg, JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id);
        }

        class Cls1
        {
            public string thename;
            public Cls2[] theclasses;
        }

        class Cls2
        {
            public string thename;
            public Cls3[] tags1;
            public Cls3[] tags2;
        }

        class Cls3
        {
            public string tagName;
        }

        [Test]
        public async Task CountObjectGraphAllocations_ForProperties()
        {
            // arrange
            // act
            await FullyJoinedQuery<object>()
                .Map(x => new Cls1
                {
                    thename = x.ThePerson.Name,
                    theclasses = x.TheClasses
                        .Select(cl => new Cls2
                        {
                            thename = cl.Name,
                            tags1 = x.TheTags
                                .Select(z => new Cls3 { tagName = z.Name })
                                .ToArray(),
                            tags2 = x.TheTags
                                .Select(z => new Cls3 { tagName = z.Name })
                                .ToArray()
                        })
                        .ToArray()
                })
                .ToArrayAsync(Executor, null, logger: Logger);

            // assert
            var debugCount = Logger.DebugMessages
                .Where(m => m.Contains(((int)LogMessages.CreatedObjectGraphAllocation).ToString()))
                .Count();

            Assert.AreEqual(3, debugCount, "3 objects represents 3 levels of properties");
        }

        [Test]
        public async Task CountObjectGraphAllocations_ForConstructorArgs()
        {
            // arrange
            // act
            var tt= await FullyJoinedQuery<object>()
                .Map(x => new
                {
                    name = x.ThePerson.Name,
                    classes = x.TheClasses
                        .Select(cl => new
                        {
                            name = cl.Name,
                            tags1 = x.TheTags
                                .Select(z => new { tagName = z.Name })
                                .ToArray(),
                            tags2 = x.TheTags
                                .Select(z => new { tagName = z.Name })
                                .ToArray()
                        })
                        .ToArray()
                })
                .ToArrayAsync(Executor, null, logger: Logger);

            // assert
            var debugCount = Logger.DebugMessages
                .Where(m => m.Contains(((int)LogMessages.CreatedObjectGraphAllocation).ToString()))
                .Count();

            Assert.AreEqual(3, debugCount, "3 objects represents 3 levels of properties");
        }

        // See todo in ComplexMapBuilder.BuildMapForConstructor

        // [Test]
        // public async Task SimpleMapReturningEmptyObject()
        // {
        //     // arrange
        //     // act
        //     var data = await FullyJoinedQuery<object>()
        //         .Map(p => new object())
        //         .ToIEnumerableAsync(Executor, null, logger: Logger);

        //     // assert
        //     Assert.AreEqual(2, data.Count());
        //     Assert.AreEqual(typeof(object), data.First().GetType());
        //     Assert.AreEqual(typeof(object), data.ElementAt(1).GetType());
        // }

        [Test]
        public async Task SimpleMapOn1FullTable()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.ThePerson)
                .ToIEnumerableAsync(Executor, null, logger: Logger);
                
            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1));
        }

        [Test]
        public async Task SimpleMapOn1Table()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.ThePerson.Name)
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Name, data.First());
            Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1));
        }
        
        [Test]
        public async Task ReturnOneFromMap()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Where(q => q.ThePersonClasses.One().ClassId == Data.Classes.Tennis.Id)
                .Map(p => p.ThePersonClasses.One())
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis, data.First());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.ElementAt(1));
        }

        [Test]
        public async Task ReturnMultipleFromMap()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.ThePersonClasses)
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(2, data.First().Count());
            Assert.AreEqual(1, data.ElementAt(1).Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis, data.First().First());
            Assert.AreEqual(Data.PersonClasses.JohnArchery, data.First().ElementAt(1));
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.ElementAt(1).First());
        }

        public class TableWithOneColumnMapper1
        {
            public TableWithOneColumn Tab { get; set; }
        }

        [Test]
        public async Task ReturnMultipleFromMap_With1Column_1()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<TableWithOneColumnMapper1>()
                .From(x => x.Tab)
                .Map(x => x.Tab)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(Data.TablesWithOneColumn, data);
        }

        public class TableWithOneColumnMapper2
        {
            public Person ThePerson { get; set; }
            public IEnumerable<TableWithOneColumn> Tabs { get; set; }
        }

        [Test]
        public void ReturnMultipleFromMap_With1Column_2()
        {
            // arrange
            // act
            var data = Sql.Query.Sqlite<TableWithOneColumnMapper2>()
                .From(x => x.ThePerson)
                .InnerJoin(q => q.Tabs).On((q, t) => q.ThePerson.Id == Data.People.John.Id)
                .Where(q => q.ThePerson.Id == Data.People.John.Id)
                .Map(x => x.Tabs)
                .ToIEnumerable(Executor, logger: Logger)
                .SelectMany(x => x);

            // assert
            CollectionAssert.AreEqual(Data.TablesWithOneColumn, data);
        }

        class PreMapped 
        {
            public long ClassId;
            public long PersonId;
            public PreMapped AnotherPreMapped;

            public PreMapped(){}

            public PreMapped(PreMapped anotherPreMapped) { AnotherPreMapped = anotherPreMapped; }

            public PreMapped(long personId) { PersonId = personId; }
        }

        [Test]
        public async Task ReturnMultipleFromMap_PreMappedWithSimpleProperty()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.ThePersonClasses.Select(pc => new PreMapped { ClassId = pc.ClassId }))
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count(), "1");
            Assert.AreEqual(2, data.ElementAt(0).Count(), "2");
            Assert.AreEqual(1, data.ElementAt(1).Count(), "3");
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data.ElementAt(0).ElementAt(0).ClassId, "4");
            Assert.AreEqual(Data.PersonClasses.JohnArchery.ClassId, data.ElementAt(0).ElementAt(1).ClassId, "5");
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data.ElementAt(1).ElementAt(0).ClassId, "6");
        }

        [Test]
        public async Task ReturnMultipleFromMap_PreMappedWithSimpleConstructorArg()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.ThePersonClasses.Select(pc => new PreMapped(pc.PersonId)).ToList())
                .ToListAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count, "1");
            Assert.AreEqual(2, data[0].Count(), "2");
            Assert.AreEqual(1, data[1].Count(), "3");
            Assert.AreEqual(Data.PersonClasses.JohnTennis.PersonId, data[0][0].PersonId, "4");
            Assert.AreEqual(Data.PersonClasses.JohnArchery.PersonId, data[0][1].PersonId, "5");
            Assert.AreEqual(Data.PersonClasses.MaryTennis.PersonId, data[1][0].PersonId, "6");
        }

        [Test]
        public async Task ReturnMultipleFromMap_PreMappedWithSimplePropertyAndSimpleConstructorArg()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.ThePersonClasses.Select(pc => new PreMapped(pc.PersonId) { ClassId = pc.ClassId }).ToList())
                .ToListAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count, "1");
            Assert.AreEqual(2, data[0].Count(), "2");
            Assert.AreEqual(1, data[1].Count(), "3");
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data[0][0].ClassId, "4 1");
            Assert.AreEqual(Data.PersonClasses.JohnTennis.PersonId, data[0][0].PersonId, "4 2");
            Assert.AreEqual(Data.PersonClasses.JohnArchery.ClassId, data[0][1].ClassId, "5 1");
            Assert.AreEqual(Data.PersonClasses.JohnArchery.PersonId, data[0][1].PersonId, "5 2");
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data[1][0].ClassId, "6 1");
            Assert.AreEqual(Data.PersonClasses.MaryTennis.PersonId, data[1][0].PersonId, "6 2");
        }

        // See todo in ComplexMapBuilder.BuildMapForConstructor

        // [Test]
        // public async Task ReturnMultipleFromMap_PreMappedWithComplexProperty()
        // {
        //     // arrange
        //     // act
        //     var data = await FullyJoinedQuery<object>()
        //         .Map(p => p.PersonClasses.Select(pc => new PreMapped { AnotherPreMapped = new PreMapped() }).ToList())
        //         .ToListAsync(Executor, null, logger: Logger);

        //     // assert
        //     Assert.AreEqual(2, data.Count, "1");
        //     Assert.AreEqual(2, data[0].Count(), "2");
        //     Assert.AreEqual(1, data[1].Count(), "3");
        //     Assert.NotNull(data[0][0].AnotherPreMapped, "4");
        //     Assert.NotNull(data[0][1].AnotherPreMapped, "5");
        //     Assert.NotNull(data[1][0].AnotherPreMapped, "6");
        // }

        // [Test]
        // public async Task ReturnMultipleFromMap_PreMappedWithSimpleConstructorArg()
        // {
        //     // arrange
        //     // act
        //     var data = await FullyJoinedQuery<object>()
        //         .Map(p => p.PersonClasses.Select(pc => new PreMapped(pc.PersonId)).ToList())
        //         .ToListAsync(Executor, null, logger: Logger);

        //    // assert may not be 100% correct

        //     // assert
        //     Assert.AreEqual(2, data.Count, "1");
        //     Assert.AreEqual(2, data[0].Count(), "2");
        //     Assert.AreEqual(1, data[1].Count(), "3");
        //     Assert.AreEqual(Data.PersonClasses.JohnTennis.PersonId, data[0][0].PersonId, "4");
        //     Assert.AreEqual(Data.PersonClasses.JohnArchery.PersonId, data[0][1].PersonId, "5");
        //     Assert.AreEqual(Data.PersonClasses.MaryTennis.PersonId, data[1][0].PersonId, "6");
        // }

        // [Test]
        // public async Task ReturnMultipleFromMap_PreMappedWithSimplePropertyAndSimpleConstructorArg()
        // {
        //     // arrange
        //     // act
        //     var data = await FullyJoinedQuery<object>()
        //         .Map(p => p.PersonClasses.Select(pc => new PreMapped(pc.PersonId) { ClassId = pc.ClassId }).ToList())
        //         .ToListAsync(Executor, null, logger: Logger);

        //    // assert may not be 100% correct

        //     // assert
        //     Assert.AreEqual(2, data.Count, "1");
        //     Assert.AreEqual(2, data[0].Count(), "2");
        //     Assert.AreEqual(1, data[1].Count(), "3");
        //     Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data[0][0].ClassId, "4 1");
        //     Assert.AreEqual(Data.PersonClasses.JohnTennis.PersonId, data[0][0].PersonId, "4 2");
        //     Assert.AreEqual(Data.PersonClasses.JohnArchery.ClassId, data[0][1].ClassId, "5 1");
        //     Assert.AreEqual(Data.PersonClasses.JohnArchery.PersonId, data[0][1].PersonId, "5 2");
        //     Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data[1][0].ClassId, "6 1");
        //     Assert.AreEqual(Data.PersonClasses.MaryTennis.PersonId, data[1][0].PersonId, "6 2");
        // }

        // [Test]
        // public async Task ReturnMultipleFromMap_PreMappedWithNoPropertiesOrConstructorArgs()
        // {
        //     // arrange
        //     // act
        //     var data = await FullyJoinedQuery<object>()
        //         .Map(p => p.PersonClasses.Select(pc => new PreMapped()).ToList())
        //         .ToListAsync(Executor, null, logger: Logger);

        //     // assert
        //     Assert.AreEqual(2, data.Count, "1");
        //     Assert.AreEqual(2, data[0].Count(), "2");
        //     Assert.AreEqual(1, data[1].Count(), "3");
        // }

        [Test]
        public async Task ReturnOneSubPropFromMap()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Where(q => q.ThePersonClasses.One().ClassId == Data.Classes.Tennis.Id)
                .Map(p => p.ThePersonClasses.One().ClassId)
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data.First());
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data.ElementAt(1));
        }

        [Test]
        public async Task ReturnOneSubPropFromMap2()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Where(q => q.ThePersonClasses.One().ClassId == Data.Classes.Tennis.Id)
                .Map(p => p.ThePersonClasses.Select(x => x.ClassId).One())
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data.First());
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data.ElementAt(1));
        }

        [Test]
        public async Task ReturnMultipleSubPropsFromMap()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.ThePersonClasses.Select(pc => pc.ClassId))
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(2, data.First().Count());
            Assert.AreEqual(1, data.ElementAt(1).Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data.First().First());
            Assert.AreEqual(Data.PersonClasses.JohnArchery.ClassId, data.First().ElementAt(1));
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data.ElementAt(1).First());
        }

        [Test]
        public async Task MapAndReturnConstant()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => 77)
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First());
            Assert.AreEqual(77, data.ElementAt(1));
        }

        [Test]
        public async Task MapAndReturnArg1()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<int>()
                .Map((p, a) => a)
                .ToIEnumerableAsync(Executor, 77, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First());
            Assert.AreEqual(77, data.ElementAt(1));
        }

        class AnInt
        {
            public int IntValue;
        }

        [Test]
        public async Task MapAndReturnArg2()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<AnInt>()
                .Map((p, a) => a.IntValue)
                .ToIEnumerableAsync(Executor, new AnInt { IntValue = 77 }, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First());
            Assert.AreEqual(77, data.ElementAt(1));
        }

        [Test]
        public async Task MapAndReturnMappedConstant()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => new Person { Id = 77 })
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(77, data.First().Id);
            Assert.AreEqual(77, data.ElementAt(1).Id);
        }

        [Test]
        public async Task SimpleMapWithAddition_PreservesOperatorPrecedence()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<Person>()
                .From()
                .Map(p => p.Id + 1 == Data.People.John.Id + 1)
                .ToListAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] {true, false}, data);
        }

        [Test]
        public async Task MapWithAddition()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => p.ThePerson.Id + 1)
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Id + 1, data.First());
            Assert.AreEqual(Data.People.Mary.Id + 1, data.ElementAt(1));
        }

        [Test]
        public async Task MapWithAddition_SwappedAround()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => 1 + p.ThePerson.Id)
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Id + 1, data.First());
            Assert.AreEqual(Data.People.Mary.Id + 1, data.ElementAt(1));
        }

        [Test]
        public async Task MapWithAddition2()
        {
            // arrange
            var one = 1;

            // act
            var data = await FullyJoinedQuery<int>()
                .Map((p, a) => p.ThePerson.Id + one + a)
                .ToIEnumerableAsync(Executor, 10, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Id + 11, data.First());
            Assert.AreEqual(Data.People.Mary.Id + 11, data.ElementAt(1));
        }

        [Test]
        public async Task AdditionInWhere()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<Person>()
                .From()
                .Where(p => p.Id + 1 == Data.People.John.Id + 1)
                .ToListAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] {Data.People.John}, data);
        }

        [Test]
        public async Task AdditionInJoin()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id + 1 == pc.PersonId + 1)
                .Where(p => p.ThePerson.Id == Data.People.John.Id)
                .ToListAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] {Data.People.John}, data.Select(d => d.ThePerson));
            CollectionAssert.AreEqual(new [] {Data.PersonClasses.JohnArchery, Data.PersonClasses.JohnTennis}, data.SelectMany(d => d.ThePersonClasses));
        }

        [Test]
        public async Task MapWithAddition_InComplexObject()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => new
                {
                    pid = p.ThePerson.Id + 1,
                    classes = p.ThePersonClasses
                        .Select(pc => pc.ClassId + p.TheClasses.One().Id)
                        .ToList()
                })
                .ToArrayAsync(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John.Id + 1, data[0].pid);
            CollectionAssert.AreEqual(new [] {Data.Classes.Tennis.Id * 2, Data.Classes.Archery.Id * 2}, data[0].classes);

            Assert.AreEqual(Data.People.Mary.Id + 1, data[1].pid);
            CollectionAssert.AreEqual(new [] {Data.Classes.Tennis.Id * 2}, data[1].classes);
        }

        void Map_SimpleBinaryConditionWorker<TOutput>(ExpressionType type, object equality, object result)
        {
            // arrange
            var exprInput = Expression.Parameter(typeof(JoinedQueryClass));

            // p => p.ThePerson.Id + result
            var mapper = Expression.Lambda<Func<JoinedQueryClass, TOutput>>(
                Expression.MakeBinary(
                    type, 
                    Expression.Property(
                        Expression.Property(
                            exprInput,
                            "ThePerson"), 
                        "Id"),
                    Expression.Constant(equality)),
                    exprInput);

            // act
            var data = FullyJoinedQuery<object>()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(mapper)
                .ToIEnumerable(Executor, null, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(result, data.First());
        }

        [Test]
        [TestCase(ExpressionType.Add, 2L, 3L, typeof(long))]
        [TestCase(ExpressionType.Subtract, 2L, -1L, typeof(long))]
        [TestCase(ExpressionType.Multiply, 2L, 2L, typeof(long))]
        [TestCase(ExpressionType.Divide, 2L, 0L, typeof(long))]
        [TestCase(ExpressionType.Equal, 2L, false, typeof(bool))]
        [TestCase(ExpressionType.NotEqual, 2L, true, typeof(bool))]
        [TestCase(ExpressionType.GreaterThan, 2L, false, typeof(bool))]
        [TestCase(ExpressionType.GreaterThanOrEqual, 2L, false, typeof(bool))]
        [TestCase(ExpressionType.LessThan, 2L, true, typeof(bool))]
        [TestCase(ExpressionType.LessThanOrEqual, 2L, true, typeof(bool))]
        public void Map_SimpleBinaryCondition(ExpressionType type, object equality, object result, Type resultType)
        {
            ReflectionUtils
                .GetMethod(() => Map_SimpleBinaryConditionWorker<int>(ExpressionType.OnesComplement, null, null), resultType)
                .Invoke(this, new object[]{ type, equality, result });
        }

        static long Add(long x) => x + x;

        [Test]
        [TestCase(ExpressionType.Add, 3L, true, false)]
        [TestCase(ExpressionType.Subtract, -1L, true, false)]
        [TestCase(ExpressionType.Multiply, 2L, true, false)]
        [TestCase(ExpressionType.Divide, 0L, true, false)]
        [TestCase(ExpressionType.Equal, false, true, false)]
        [TestCase(ExpressionType.NotEqual, true, true, false)]
        [TestCase(ExpressionType.GreaterThan, false, true, true)]
        [TestCase(ExpressionType.GreaterThanOrEqual, false, true, false)]
        [TestCase(ExpressionType.LessThan, true, true, false)]
        [TestCase(ExpressionType.LessThanOrEqual, true, true, true)]
        public async Task Where_BinaryCondition(ExpressionType type, object result, bool john, bool mary)
        {
            // arrange
            var exprInput = Expression.Parameter(typeof(Person));

            // p => p.Id + 2 == result
            var where = Expression.Lambda<Func<Person, bool>>(
                Expression.Equal(
                    Expression.MakeBinary(
                        type, 
                        Expression.Property(
                            exprInput,
                            "Id"),
                        Expression.Constant(2L)),
                    Expression.Constant(result)),
                exprInput);

            // act
            var actual = await Sql.Query.Sqlite<Person>()
                .From()
                .Where(where)
                .ToListAsync(Executor, logger: Logger);

            // assert
            var expected = 
                (john ? Data.People.John : null).ToEnumerable()
                .Concat((mary ? Data.People.Mary: null).ToEnumerable());
            CollectionAssert.AreEqual(expected, actual);
        }

        [Test]
        [TestCase(ExpressionType.Add, 3L)]
        [TestCase(ExpressionType.Subtract, -1L)]
        [TestCase(ExpressionType.Multiply, 2L)]
        [TestCase(ExpressionType.Divide, 0L)]
        [TestCase(ExpressionType.Equal, false)]
        [TestCase(ExpressionType.NotEqual, true)]
        [TestCase(ExpressionType.GreaterThan, false)]
        [TestCase(ExpressionType.GreaterThanOrEqual, false)]
        [TestCase(ExpressionType.LessThan, true)]
        [TestCase(ExpressionType.LessThanOrEqual, true)]
        public async Task Join_BinaryCondition(ExpressionType type, object result)
        {
            // arrange
            var exprInput1 = Expression.Parameter(typeof(JoinedQueryClass));
            var exprInput2 = Expression.Parameter(typeof(PersonClass));

            // (q, pc) => q.ThePerson.Id + 2 == result
            var join = Expression.Lambda<Func<JoinedQueryClass, PersonClass, bool>>(
                Expression.Equal(
                    Expression.MakeBinary(
                        type,
                        Expression.PropertyOrField(
                            Expression.PropertyOrField(
                                exprInput1,
                                "ThePerson"),
                            "Id"),
                        Expression.Constant(2L)),
                    Expression.Constant(result)),
                exprInput1,
                exprInput2);
                
            // act
            var data = await Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses).On(join)
                .Where(p => p.ThePerson.Id == Data.People.John.Id)
                .ToListAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] {Data.People.John}, data.Select(d => d.ThePerson));
            CollectionAssert.AreEqual(new [] 
            { 
                Data.PersonClasses.JohnArchery, 
                Data.PersonClasses.JohnTennis, 
                Data.PersonClasses.MaryTennis
            }, data.SelectMany(d => d.ThePersonClasses));
        }

        [Test]
        [TestCase(ExpressionType.Add, 1L)]
        [TestCase(ExpressionType.Subtract, 1L)]
        [TestCase(ExpressionType.Multiply, 1L)]
        [TestCase(ExpressionType.Divide, 1L)]
        [TestCase(ExpressionType.Equal, true)]
        [TestCase(ExpressionType.NotEqual, true)]
        [TestCase(ExpressionType.GreaterThan, true)]
        [TestCase(ExpressionType.GreaterThanOrEqual, true)]
        [TestCase(ExpressionType.LessThan, true)]
        [TestCase(ExpressionType.LessThanOrEqual, true)]
        public async Task Join_OrderByCondition_SmokeTests(ExpressionType type, object result)
        {
            // arrange
            var exprInput = Expression.Parameter(typeof(Person));

            // p => p.Id + 2 == result
            var ob = Expression.Lambda<Func<Person, bool>>(
                Expression.Equal(
                    Expression.MakeBinary(
                        type, 
                        Expression.Property(
                            exprInput,
                            "Id"),
                        Expression.Constant(2L)),
                    Expression.Constant(result)),
                exprInput);

            // act
            var actual = await Sql.Query.Sqlite<Person>()
                .From()
                .OrderBy(ob)
                .ToListAsync(Executor, logger: Logger);

            // assert
            // assert would not provide much value and would be complex
        }

        [Test]
        [Ignore("This case")]
        public void LeftJoinReturnsNull()
        {
        }

        [Test]
        [Ignore("This case")]
        public void RightJoinReturnsNull()
        {
        }

        [Test]
        public async Task CountAndGroup()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Select(x => x.Id).Count()
                })
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 2
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 1
                }
            }, data);
        }

        class CountAndGroupTest
        {
            public string person;
            public int classes;
        }

        [Test]
        [Ignore("TODO")]
        public async Task CountAndGroup_ToProperties()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => new CountAndGroupTest
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Select(x => x.Id).Count()
                })
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 2
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 1
                }
            }, data);
        }

        [Test]
        [Ignore("TODO")]
        public async Task CountAndGroup_2()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Count()
                })
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 2
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 1
                }
            }, data);
        }

        [Test]
        [Ignore("TODO")]
        public async Task SumAndGroup_WithAdd()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery<object>()
                .Map(p => new 
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Select(c => c.Id + p.ThePersonClasses.One().ClassId).Sum()
                })
                .ToIEnumerableAsync(Executor, null, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] 
            {
                new 
                {
                    person = Data.People.John.Name,
                    classes = 14
                },
                new 
                {
                    person = Data.People.Mary.Name,
                    classes = 6
                }
            }, data);
        }
    }
}
