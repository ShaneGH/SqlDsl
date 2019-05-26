using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;
using SqlDsl.Utils;

namespace SqlDsl.UnitTests.FullPathTests
{
    /// <summary>
    /// This is a place to put tests that don't really make sense anymore, rather than delete them.
    /// </summary>
    [SqlTestAttribute(TestFlavour.Sqlite)]
    public class TestGraveyard : FullPathTestBase
    {
        public TestGraveyard(TestFlavour testFlavour)
            : base(testFlavour)
        {
        }
        
        [Test]
        public async Task MapWithAddition()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => p.ThePerson.Id + 1)
                .ToIEnumerableAsync(Executor, logger: Logger);

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
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => 1 + p.ThePerson.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

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
            var data = await TestUtils
                .FullyJoinedQueryWithArg<int>()
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
            var data = await Sql.Query.Sqlite<QueryContainer>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id + 1 == pc.PersonId + 1)
                .Where(p => p.ThePerson.Id == Data.People.John.Id)
                .ToListAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(new [] {Data.People.John}, data.Select(d => d.ThePerson));
            CollectionAssert.AreEqual(new [] {Data.PersonClasses.JohnArchery, Data.PersonClasses.JohnTennis}, data.SelectMany(d => d.ThePersonClasses));
        }

        void Map_SimpleBinaryConditionWorker<TOutput>(ExpressionType type, object equality, object result)
        {
            // arrange
            var exprInput = Expression.Parameter(typeof(QueryContainer));

            // p => p.ThePerson.Id + result
            var mapper = Expression.Lambda<Func<QueryContainer, TOutput>>(
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
            var data = TestUtils
                .FullyJoinedQuery()
                .Where(x => x.ThePerson.Id == Data.People.John.Id)
                .Map(mapper)
                .ToIEnumerable(Executor, logger: Logger);

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
            var exprInput1 = Expression.Parameter(typeof(QueryContainer));
            var exprInput2 = Expression.Parameter(typeof(PersonClass));

            // (q, pc) => q.ThePerson.Id + 2 == result
            var join = Expression.Lambda<Func<QueryContainer, PersonClass, bool>>(
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
            var data = await Sql.Query.Sqlite<QueryContainer>()
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
                .OrderBy(ob)
                .ToListAsync(Executor, logger: Logger);

            // assert
            // assert would not provide much value and would be complex
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
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => p.ThePersonClasses.Select(pc => new PreMapped { ClassId = pc.ClassId }))
                .ToIEnumerableAsync(Executor, logger: Logger);

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
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => p.ThePersonClasses.Select(pc => new PreMapped(pc.PersonId)).ToList())
                .ToListAsync(Executor, logger: Logger);

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
            var data = await TestUtils
                .FullyJoinedQuery()
                .Map(p => p.ThePersonClasses
                    .Select(pc => new PreMapped(pc.PersonId)
                    { 
                        ClassId = pc.ClassId
                    })
                    .ToList())
                .ToListAsync(Executor, logger: Logger);

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
    }
}