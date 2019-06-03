using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using SqlDsl.Mapper;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(SqlType.MySql)]
    [SqlTestAttribute(SqlType.Sqlite)]
    public class MappingObjectShapeTests : FullPathTestBase
    {
        public MappingObjectShapeTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }

        public class TableWithOneColumnMapper1
        {
            public TableWithOneRowAndOneColumn Tab { get; set; }
        }

        [Test]
        public async Task ReturnMultipleFromMap_With1Column_1()
        {
            // arrange
            // act
            var data = await Query<TableWithOneColumnMapper1>()
                .From(x => x.Tab)
                .Map(x => x.Tab)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            CollectionAssert.AreEqual(Data.TablesWithOneRowAndOneColumn, data);
        }

        public class TableWithOneColumnMapper2
        {
            public Person ThePerson { get; set; }
            public IEnumerable<TableWithOneRowAndOneColumn> Tabs { get; set; }
        }

        [Test]
        public void ReturnMultipleFromMap_With1Column_2()
        {
            // arrange
            // act
            var data = Query<TableWithOneColumnMapper2>()
                .From(x => x.ThePerson)
                .InnerJoinMany(q => q.Tabs).On((q, t) => q.ThePerson.Id == Data.People.John.Id)
                .Where(q => q.ThePerson.Id == Data.People.John.Id)
                .Map(x => x.Tabs)
                .ToIEnumerable(Executor, logger: Logger)
                .SelectMany(x => x);

            // assert
            CollectionAssert.AreEqual(Data.TablesWithOneRowAndOneColumn, data);
        }

        // See todo in ComplexMapBuilder.BuildMapForConstructor

        // [Test]
        // public async Task SimpleMapReturningEmptyObject()
        // {
        //     // arrange
        //     // act
        //     var data = await FullyJoinedQuery<object>()
        //         .Map(p => new object())
        //         .ToIEnumerableAsync(Executor, logger: Logger);

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
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(p => p.ThePerson)
                .ToIEnumerableAsync(Executor, logger: Logger);
                
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
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(p => p.ThePerson.Name)
                .ToIEnumerableAsync(Executor, logger: Logger);

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
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Where(q => q.ThePersonClasses.One().ClassId == Data.Classes.Tennis.Id)
                .Map(p => p.ThePersonClasses.One())
                .ToIEnumerableAsync(Executor, logger: Logger);

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
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(p => p.ThePersonClasses)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(2, data.First().Count());
            Assert.AreEqual(1, data.ElementAt(1).Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis, data.First().First());
            Assert.AreEqual(Data.PersonClasses.JohnArchery, data.First().ElementAt(1));
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.ElementAt(1).First());
        }

        class QueryClass1
        {
            public Person ThePerson { get; set; }
            public PersonsData ThePersonsData { get; set; }
            public IEnumerable<PersonClass> ThePersonClasses { get; set; }
        }
        
        class QueryClass2
        {
            public QueryClass1 Inner { get; set; }
        }
        
        class QueryClass3
        {
            public List<QueryClass1> Inner { get; set; }
        }

        [Test]
        public async Task SelectWith0Levels()
        {
            // arrange
            // act
            var data = await Query<Person>()
                .From(result => result)
                .ToIEnumerableAsync(Executor, logger: Logger);
                
            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1));
        }

        [Test]  
        public async Task SelectWith0LevelsAndMap()
        {
            // arrange
            // act
            var data = await Query<Person>()
                .From(result => result)
                .Map(x => x.Id)
                .ToListAsync(Executor, logger: Logger);
                
            // assert
            Assert.AreEqual(2, data.Count);
            Assert.AreEqual(Data.People.John.Id, data[0]);
            Assert.AreEqual(Data.People.Mary.Id, data[1]);
        }

        [Test]
        public async Task SelectWith1Levels()
        {
            // arrange
            // act
            var data = await Query<QueryClass1>()
                .From(result => result.ThePerson)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().ThePerson);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).ThePerson);
        }

        [Test]
        public async Task SelectWith1Level_ReturnsArrayDataType()
        {
            // arrange
            // act
            var data = await Query<QueryClass1>()
                .From(result => result.ThePerson)
                .InnerJoinOne(x => x.ThePersonsData).On((q, d) => q.ThePerson.Id == d.PersonId)
                .Map(x => x.ThePersonsData.Data)
                .ToListAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count);
            CollectionAssert.AreEqual(Data.PeoplesData.JohnsData.Data, data[0]);
            CollectionAssert.AreEqual(Data.PeoplesData.MarysData.Data, data[1]);
        }

        [Test]
        public async Task SelectWith2Levels()
        {
            // arrange
            // act
            var data = await Query<QueryClass2>()
                .From(result => result.Inner.ThePerson)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Inner.ThePerson);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).Inner.ThePerson);
        }

        [Test]
        public async Task WhereWith2Levels()
        {
            // arrange
            // act
            var data = await Query<QueryClass2>()
                .From(result => result.Inner.ThePerson)
                .Where(result => result.Inner.ThePerson.Id == Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(0).Inner.ThePerson);
        }

        [Test]
        public async Task JoinWith2Levels()
        {
            // arrange
            // act
            var data = await Query<QueryClass2>()
                .From(result => result.Inner.ThePerson)
                .LeftJoinMany<PersonClass>(result => result.Inner.ThePersonClasses)
                    .On((r, pc) => r.Inner.ThePerson.Id == pc.PersonId)
                .Where(result => result.Inner.ThePerson.Id == Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(0).Inner.ThePerson);
            Assert.AreEqual(1, data.First().Inner.ThePersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.First().Inner.ThePersonClasses.First());
        }

        [Test]
        public async Task JoinWith2LevelsAndList()
        {
            // arrange
            // act
            var data = await Query<QueryClass3>()
                .From(result => result.Inner.SingleOrDefault().ThePerson)
                .LeftJoinMany<PersonClass>(result => result.Inner.One().ThePersonClasses)
                    .On((r, pc) => r.Inner.FirstOrDefault().ThePerson.Id == pc.PersonId)
                .Where(result => result.Inner.One().ThePerson.Id == Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(1, data.ElementAt(0).Inner.Count());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(0).Inner.First().ThePerson);
            Assert.AreEqual(1, data.First().Inner.First().ThePersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.First().Inner.First().ThePersonClasses.First());
        }

        class QueryClass5
        {
            public Person ThePerson { get; set; }
            public PersonClass ThePersonClass { get; set; }
        }

        [Test]
        public async Task JoinTableIsNotList_1ResultReturned_MapsCorrectly()
        {
            // arrange
            // act
            var data = await Query<QueryClass5>()
                .From(result => result.ThePerson)
                .LeftJoinOne<PersonClass>(result => result.ThePersonClass)
                    .On((r, pc) => r.ThePerson.Id == pc.PersonId)
                .Where(result => result.ThePerson.Id == Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.First().ThePerson);
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.First().ThePersonClass);
        }

        [Test]
        public void JoinTableIsNotList_MoreThan1ResultReturned_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(InvalidOperationException), () => Query<QueryClass5>()
                .From(result => result.ThePerson)
                .LeftJoinOne<PersonClass>(result => result.ThePersonClass)
                    .On((r, pc) => r.ThePerson.Id == pc.PersonId)
                .Where(result => result.ThePerson.Id == Data.People.John.Id)
                .ToListAsync(Executor));
        }

        class WhereErrorQueryClass
        {
            // warning CS0649: Field 'ObjectShapeTests.WhereErrorQueryClass.Person2' is never assigned to, and will always have its default value null
            #pragma warning disable 0649
            public Person Person1;
            public Person Person2;
            #pragma warning restore 0649
        }

        [Test]
        public void Select_WhereComparrisonComparesComplexObjects_ThrowsError()
        {
            // TODO: this test is all good, but the exception message returned could 
            // be better

            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(SqlBuilderException), () =>
                Query<WhereErrorQueryClass>()
                    .From(result => result.Person1)
                    .InnerJoinOne(result => result.Person2)
                        .On((q, p) => q.Person1.Id == p.Id)
                    .Where(q => q.Person1 == q.Person2)
                    .ToIEnumerableAsync(Executor));
        }

        [Test]
        public async Task AnonymousObjects_Simple()
        {
            // arrange
            // act
            var data = await Query<QueryContainer>()
                .From(result => result.ThePerson)
                .Where(result => result.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    name = q.ThePerson.Name,
                    person = q.ThePerson
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            Assert.AreEqual(Data.People.John.Name, john.name);
            Assert.AreEqual(Data.People.John, john.person);
        }

        [Test]
        public async Task AnonymousObjects_Complex()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Where(result => result.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    name = q.ThePerson.Name,
                    person = q.ThePerson,
                    classes = q.TheClasses
                        .Select(c => new
                        {
                            className = c.Name,
                            tags = q.TheTags
                                .Select(t => t.Name)
                                .ToArray()
                        })
                        .ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            Assert.AreEqual(Data.People.John.Name, john.name);
            Assert.AreEqual(Data.People.John, john.person);
            
            Assert.AreEqual(2, john.classes.Length);
            
            Assert.AreEqual(Data.Classes.Tennis.Name, john.classes[0].className);
            CollectionAssert.AreEqual(new [] { Data.Tags.Sport.Name, Data.Tags.BallSport.Name }, john.classes[0].tags);
            
            Assert.AreEqual(Data.Classes.Archery.Name, john.classes[1].className);
            CollectionAssert.AreEqual(new [] { Data.Tags.Sport.Name }, john.classes[1].tags);
        }

        [Test]
        public async Task ReturnMultipleSubPropsFromMap()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(p => p.ThePersonClasses.Select(pc => pc.ClassId))
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(2, data.First().Count());
            Assert.AreEqual(1, data.ElementAt(1).Count());
            Assert.AreEqual(Data.PersonClasses.JohnTennis.ClassId, data.First().First());
            Assert.AreEqual(Data.PersonClasses.JohnArchery.ClassId, data.First().ElementAt(1));
            Assert.AreEqual(Data.PersonClasses.MaryTennis.ClassId, data.ElementAt(1).First());
        }

        [Test]
        public async Task ReturnMultipleSubPropsInComplexObjFromMap()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(p => p.ThePersonClasses.Select(pc => new { cid = pc.ClassId }))
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(2, data.First().Count());
            Assert.AreEqual(1, data.ElementAt(1).Count());
            Assert.AreEqual(new { cid = Data.PersonClasses.JohnTennis.ClassId }, data.First().First());
            Assert.AreEqual(new { cid = Data.PersonClasses.JohnArchery.ClassId }, data.First().ElementAt(1));
            Assert.AreEqual(new { cid = Data.PersonClasses.MaryTennis.ClassId }, data.ElementAt(1).First());
        }

        [Test]
        public async Task ValueTuplesInQueryFirstPart()
        {
            // arrange
            // act
            var data = await Query<(Person person, PersonClass[] personClasses, Class[] classes)>()
                .From(result => result.person)
                .LeftJoinMany<PersonClass>(result => result.personClasses)
                    .On((r, pc) => r.person.Id == pc.PersonId)
                .LeftJoinMany<Class>(result => result.classes)
                    .On((r, pc) => r.personClasses.One().ClassId == pc.Id)
                .Where(result => result.person.Id == Data.People.John.Id)
                .Map(q => new
                {
                    name = q.person.Name,
                    person = q.person,
                    classes = q.classes
                        .Select(c => new
                        {
                            className = c.Name
                        })
                        .ToArray()
                })
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            var john = data.First();
            Assert.AreEqual(Data.People.John.Name, john.name);
            Assert.AreEqual(Data.People.John, john.person);
            
            Assert.AreEqual(2, john.classes.Length);
            
            Assert.AreEqual(Data.Classes.Tennis.Name, john.classes[0].className);
            
            Assert.AreEqual(Data.Classes.Archery.Name, john.classes[1].className);
        }

        [Test]
        public async Task ObjectWithConstructorArgs1()
        {
            // arrange
            // act
            var data = await Query<Person>()
                .Map(x => new Person(x.Id, x.Name, x.Gender, x.IsMember))
                .ToIEnumerableAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1));
        }

        class ObjectWithConstructorArgs2Test
        {
            public readonly Person TheClassPerson;

            public ObjectWithConstructorArgs2Test(Person person)
            {
                TheClassPerson = person;
            }
        }

        [Test]
        public async Task ObjectWithConstructorArgs2()
        {
            // arrange
            // act
            var data = await Query<Person>()
                .Map(x => new ObjectWithConstructorArgs2Test(x))
                .ToIEnumerableAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().TheClassPerson);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).TheClassPerson);
        }

        [Test]
        public async Task ObjectWithConstructorArgs3()
        {
            // arrange
            // act
            var data = await Query<QueryClass1>()
                .From(x => x.ThePerson)
                .Map(x => new ObjectWithConstructorArgs2Test(x.ThePerson))
                .ToIEnumerableAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().TheClassPerson);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).TheClassPerson);
        }

        class ObjectWithConstructorArgs4Test
        {
            public readonly byte[] Datas;

            public ObjectWithConstructorArgs4Test(byte[] data)
            {
                Datas = data;
            }
        }

        [Test]
        public async Task ObjectWithConstructorArgs4()
        {
            // arrange
            // act
            var data = await Query<PersonsData>()
                .Map(x => new ObjectWithConstructorArgs4Test(x.Data))
                .ToIEnumerableAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.PeoplesData.JohnsData.Data, data.First().Datas);
            Assert.AreEqual(Data.PeoplesData.MarysData.Data, data.ElementAt(1).Datas);
        }

        class ObjectWithConstructorArgs5Test
        {
            public readonly List<byte> Datas;

            public ObjectWithConstructorArgs5Test(List<byte> data)
            {
                Datas = data;
            }
        }

        [Test]
        public async Task ObjectWithConstructorArgs5()
        {
            // arrange
            // act
            var data = await Query<PersonsData>()
                .Map(x => new ObjectWithConstructorArgs5Test(x.Data.ToList()))
                .ToIEnumerableAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.PeoplesData.JohnsData.Data, data.First().Datas);
            Assert.AreEqual(Data.PeoplesData.MarysData.Data, data.ElementAt(1).Datas);
        }

        class ObjectWithConstructorArgs6Test
        {
            public Person PersonWithName { get; set; }
            public Person PersonWithGender { get; set; }

            public ObjectWithConstructorArgs6Test Inner { get; set; }
        }

        [Test]
        public async Task ObjectWithConstructorArgs_NestedObjectsAndMultipleCArgsObjectsOnOneLevel()
        {
            // arrange
            // act
            var data = await Query<Person>()
                .Map(x => new ObjectWithConstructorArgs6Test
                {
                    PersonWithName = new Person(0, x.Name, 0, false),
                    PersonWithGender = new Person(0, null, x.Gender, false),
                    Inner = new ObjectWithConstructorArgs6Test
                    {
                        PersonWithName = new Person(0, x.Name, 0, false),
                        PersonWithGender = new Person(0, null, x.Gender, false),
                    }
                })
                .ToIEnumerableAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count());
            
            Assert.AreEqual(new Person(0, "John", 0, false), data.First().PersonWithName);
            Assert.AreEqual(new Person(0, null, Gender.Male, false), data.First().PersonWithGender);
            Assert.AreEqual(new Person(0, "John", 0, false), data.First().Inner.PersonWithName);
            Assert.AreEqual(new Person(0, null, Gender.Male, false), data.First().Inner.PersonWithGender);
            
            Assert.AreEqual(new Person(0, "Mary", 0, false), data.ElementAt(1).PersonWithName);
            Assert.AreEqual(new Person(0, null, Gender.Female, false), data.ElementAt(1).PersonWithGender);
            Assert.AreEqual(new Person(0, "Mary", 0, false), data.ElementAt(1).Inner.PersonWithName);
            Assert.AreEqual(new Person(0, null, Gender.Female, false), data.ElementAt(1).Inner.PersonWithGender);
        }

        class ObjectWithConstructorArgs_OuterSelectTest
        {
            public readonly IEnumerable<Class> TheClasses;

            public ObjectWithConstructorArgs_OuterSelectTest(IEnumerable<Class> classes)
            {
                TheClasses = classes;
            }
        }

        [Test]
        public async Task ObjectWithConstructorArgs_OuterSelect()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(x => new
                {
                    personName = x.ThePerson.Name,
                    classes = new ObjectWithConstructorArgs_OuterSelectTest(x.TheClasses)
                })
                .ToIEnumerableAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count());
            
            Assert.AreEqual(Data.People.John.Name, data.First().personName);
            CollectionAssert.AreEqual(new [] { Data.Classes.Tennis, Data.Classes.Archery }, data.First().classes.TheClasses);
            
            Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1).personName);
            CollectionAssert.AreEqual(new [] { Data.Classes.Tennis }, data.ElementAt(1).classes.TheClasses);
        }

        class ObjectWithConstructorArgs_InnerSelectTest : EqComparer
        {
            public readonly Class TheClass;

            public ObjectWithConstructorArgs_InnerSelectTest(Class theClass)
            {
                TheClass = theClass;
            }

            public override int GetHashCode() => TheClass.GetHashCode();
            public override bool Equals(object p)
            {
                var cls = (p as ObjectWithConstructorArgs_InnerSelectTest)?.TheClass;
                return cls == TheClass;
            }
        }

        [Test]
        public async Task ObjectWithConstructorArgs_InnerSelect()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(x => new
                {
                    personName = x.ThePerson.Name,
                    classes = x.TheClasses
                        .Select(c => new ObjectWithConstructorArgs_InnerSelectTest(c))
                })
                .ToIEnumerableAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count());
            
            Assert.AreEqual(Data.People.John.Name, data.First().personName);
            CollectionAssert.AreEqual(new [] 
            {
                new ObjectWithConstructorArgs_InnerSelectTest(Data.Classes.Tennis), 
                new ObjectWithConstructorArgs_InnerSelectTest(Data.Classes.Archery)
            }, data.First().classes);
            
            Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1).personName);
            CollectionAssert.AreEqual(new [] 
            { 
                new ObjectWithConstructorArgs_InnerSelectTest(Data.Classes.Tennis)
            }, data.ElementAt(1).classes);
        }

        [Test]
        public async Task SelectWithoutColumns_MapsCorrectly()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Map(x => new
                {
                    personName = x.ThePerson.Name,
                    classes = x.TheClasses.Select(c => 1),
                    tags = x.TheTags.Select(t => 2)
                })
                .ToListAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count());

            Assert.AreEqual(Data.People.John.Name, data[0].personName);
            CollectionAssert.AreEqual(new[]{1, 1}, data[0].classes);
            CollectionAssert.AreEqual(new[]{2, 2, 2}, data[0].tags);
            
            Assert.AreEqual(Data.People.Mary.Name, data[1].personName);
            CollectionAssert.AreEqual(new[]{1}, data[1].classes);
            CollectionAssert.AreEqual(new[]{2, 2}, data[1].tags);
        }

        [Test]
        public async Task SelectWithColumnsAndAddition_MapsCorrectly()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Where(c => c.ThePerson.Id == Data.People.John.Id)
                .Map(x => new
                {
                    personName = x.ThePerson.Name,
                    classes = x.TheClasses.Select(c => c.Id + 1),
                    tags = x.TheTags.Select(t => t.Id + 1)
                })
                .ToListAsync(Executor);

            // assert
            Assert.AreEqual(1, data.Count());

            Assert.AreEqual(Data.People.John.Name, data[0].personName);
            CollectionAssert.AreEquivalent(new[]
            {
                Data.Classes.Tennis.Id + 1, 
                Data.Classes.Archery.Id + 1
            }, data[0].classes);

            CollectionAssert.AreEquivalent(new[]
            {
                Data.Tags.Sport.Id + 1,
                Data.Tags.BallSport.Id + 1,
                Data.Tags.Sport.Id + 1,
            }, data[0].tags);
        }

        [Test]
        public async Task FullyJoinedQuery_WhereTableNotInSelect_MapsCorrectly()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Where(x => x.ThePersonsData.PersonId == 555L)
                .Map(x => new
                {
                    person = x.ThePerson.Name
                })
                .ToListAsync(Executor);

            // assert
            Assert.AreEqual(0, data.Count);

            // TODO: this test verifies that if a column is present in the WHERE
            // statement, but it (or none of it's sibling columns) is not present in the 
            // map, then the SqlStatementBuilder.FilterUnusedTables does not remove it
            // from the query.

            // Need also to write a test for the JOIN ON (...) part, but first,
            // will need to suport multi dimentional joins. This is because single 
            // dimentional joins are handled correctly for a different reason
        }

        [Test]
        public async Task FullyJoinedQuery_OrderByTableNotInSelect_MapsCorrectly()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .OrderBy(x => x.ThePersonsData.PersonId)
                .Map(x => new
                {
                    person = x.ThePerson.Name
                })
                .ToListAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count);
        }

        [Test]
        public async Task BackwardsQuery()
        {
            // arrange
            // act
            var data = await Query<(IEnumerable<Person> thePerson, IEnumerable<PersonClass> thePersonClass, Class theClass)>()
                .From(x => x.theClass)
                .LeftJoinMany(q => q.thePersonClass)
                    .On((q, pc) => q.theClass.Id == pc.ClassId)
                .LeftJoinMany(q => q.thePerson)
                    .On((q, c) => q.thePersonClass.One().PersonId == c.Id)
                .Map(x => new
                {
                    person = x.thePerson.Select(p => p.Name),
                    cls = x.theClass.Name
                })
                .ToListAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count);
        }

        [Test]
        [Ignore("Does not work in MySql")]
        public async Task QueryWithJoinsInWrongOrder_ExecutesSuccessfully()
        {
            // arrange
            // act
            // assert
            await Query<QueryContainer>()
                .From(x => x.ThePerson)
                .InnerJoinMany<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id)
                .InnerJoinMany<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoinMany<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .InnerJoinMany<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .Map(x => new
                {
                    person = x.ThePerson.Name,
                    classes = x.TheClasses.Select(c => new 
                    {
                        className = c.Name,
                        tags = x.TheTags.Select(t => t.Name).ToList()
                    }).ToList()
                })
                .ToListAsync(Executor);
        }

        /// <summary>
        /// This is meant as a smoke test for other things
        /// </summary>
        [Test]
        public async Task The_Gold_Standard_WithoutValues()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Where(x => x.ThePersonsData.PersonId == 555L)
                .Map(x => new
                {
                    person = x.ThePerson.Name,
                    classes = x.TheClasses.Select(c => new 
                    {
                        className = c.Name,
                        tags = x.TheTags.Select(t => t.Name).ToList()
                    }).ToList()
                })
                .ToListAsync(Executor);

            // assert
            //Assert.Fail();
        }

        /// <summary>
        /// This is meant as a smoke test for other things
        /// </summary>
        [Test]
        public async Task The_Gold_Standard_WithValues()
        {
            // arrange
            // act
            var data = await TestUtils
                .FullyJoinedQuery(SqlType)
                .Where(x => x.ThePerson.Id == 1)
                .Map(x => new
                {
                    person = x.ThePerson.Name,
                    classes = x.TheClasses.Select(c => new 
                    {
                        className = c.Name,
                        tags = x.TheTags.Select(t => t.Name).ToList()
                    }).ToList()
                })
                .ToListAsync(Executor);

            // assert
            Assert.AreEqual(1, data.Count);
            Assert.AreEqual(2, data[0].classes.Count);
            
            Assert.AreEqual(Data.Classes.Tennis.Name, data[0].classes[0].className);
            Assert.AreEqual(new [] { Data.Tags.Sport.Name, Data.Tags.BallSport.Name }, data[0].classes[0].tags);
            
            Assert.AreEqual(Data.Classes.Archery.Name, data[0].classes[1].className);
            Assert.AreEqual(new [] { Data.Tags.Sport.Name }, data[0].classes[1].tags);
        }
    }
}
