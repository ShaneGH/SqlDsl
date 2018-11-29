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

namespace SqlDsl.UnitTests.FullPathTests
{
    [TestFixture]
    public class ObjectShapeTests : FullPathTestBase
    {
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public List<PersonClass> PersonClasses { get; set; }
            public List<Class> Classes { get; set; }
            public List<ClassTag> ClassTags { get; set; }
            public List<Tag> Tags { get; set; }
            public List<Purchase> PurchasesByMe { get; set; }
            public List<Purchase> PurchasesByMeForMyClasses { get; set; }
        }

        static Dsl.IQuery<JoinedQueryClass> FullyJoinedQuery()
        {
            return Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .LeftJoin<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .LeftJoin<Class>(q => q.Classes)
                    .On((q, c) => q.PersonClasses.One().ClassId == c.Id)
                .LeftJoin<ClassTag>(q => q.ClassTags)
                    .On((q, ct) => q.Classes.One().Id == ct.ClassId)
                .LeftJoin<Tag>(q => q.Tags)
                    .On((q, t) => q.ClassTags.One().TagId == t.Id)
                .LeftJoin<Purchase>(q => q.PurchasesByMe)
                    .On((q, t) => q.ThePerson.Id == t.PersonId);
                // .LeftJoin<Purchase>(q => q.PurchasesByMeForMyClasses)
                //     .On((q, t) => q.ThePerson.Id == t.PersonId && q.Classes.One().Id == t.ClassId);
        }

        class QueryClass1
        {
            public Person Person { get; set; }
            public IEnumerable<PersonClass> PersonClasses { get; set; }
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
            var data = await Sql.Query.Sqlite<Person>()
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
            var data = await Sql.Query.Sqlite<Person>()
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
            var data = await Sql.Query.Sqlite<QueryClass1>()
                .From(result => result.Person)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Person);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).Person);
        }

        [Test]
        public async Task SelectWith2Levels()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass2>()
                .From(result => result.Inner.Person)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(2, data.Count());
            Assert.AreEqual(Data.People.John, data.First().Inner.Person);
            Assert.AreEqual(Data.People.Mary, data.ElementAt(1).Inner.Person);
        }

        [Test]
        public async Task WhereWith2Levels()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass2>()
                .From(result => result.Inner.Person)
                .Where(result => result.Inner.Person.Id == Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(0).Inner.Person);
        }

        [Test]
        public async Task JoinWith2Levels()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass2>()
                .From(result => result.Inner.Person)
                .LeftJoin<PersonClass>(result => result.Inner.PersonClasses)
                    .On((r, pc) => r.Inner.Person.Id == pc.PersonId)
                .Where(result => result.Inner.Person.Id == Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(0).Inner.Person);
            Assert.AreEqual(1, data.First().Inner.PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.First().Inner.PersonClasses.First());
        }

        [Test]
        public async Task JoinWith2LevelsAndList()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass3>()
                .From(result => result.Inner.SingleOrDefault().Person)
                .LeftJoin<PersonClass>(result => result.Inner.One().PersonClasses)
                    .On((r, pc) => r.Inner.FirstOrDefault().Person.Id == pc.PersonId)
                .Where(result => result.Inner.One().Person.Id == Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(1, data.ElementAt(0).Inner.Count());
            Assert.AreEqual(Data.People.Mary, data.ElementAt(0).Inner.First().Person);
            Assert.AreEqual(1, data.First().Inner.First().PersonClasses.Count());
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.First().Inner.First().PersonClasses.First());
        }

        class QueryClass5
        {
            public Person Person { get; set; }
            public PersonClass PersonClass { get; set; }
        }

        [Test]
        public async Task JoinTableIsNotList_1ResultReturned_MapsCorrectly()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<QueryClass5>()
                .From(result => result.Person)
                .LeftJoin<PersonClass>(result => result.PersonClass)
                    .On((r, pc) => r.Person.Id == pc.PersonId)
                .Where(result => result.Person.Id == Data.People.Mary.Id)
                .ToIEnumerableAsync(Executor, logger: Logger);

            // assert
            Assert.AreEqual(1, data.Count());
            Assert.AreEqual(Data.People.Mary, data.First().Person);
            Assert.AreEqual(Data.PersonClasses.MaryTennis, data.First().PersonClass);
        }

        [Test]
        public void JoinTableIsNotList_MoreThan1ResultReturned_ThrowsException()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(InvalidOperationException), () => Sql.Query.Sqlite<QueryClass5>()
                .From(result => result.Person)
                .LeftJoin<PersonClass>(result => result.PersonClass)
                    .On((r, pc) => r.Person.Id == pc.PersonId)
                .Where(result => result.Person.Id == Data.People.John.Id)
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
        public void Select_JoinComparrisonComparesComplexObjects_ThrowsError()
        {
            // arrange
            // act
            // assert
            Assert.Throws(typeof(NotImplementedException), () =>
                Sql.Query.Sqlite<WhereErrorQueryClass>()
                    .From(result => result.Person1)
                    .InnerJoin(result => result.Person2)
                        .On((q, p) => q.Person1 == p)
                    .Compile());
        }

        [Test]
        public void Select_WhereComparrisonComparesComplexObjects_ThrowsError()
        {
            // arrange
            // act
            // assert
            Assert.ThrowsAsync(typeof(SqliteException), () =>
                Sql.Query.Sqlite<WhereErrorQueryClass>()
                    .From(result => result.Person1)
                    .InnerJoin(result => result.Person2)
                        .On((q, p) => q.Person1.Id == p.Id)
                    .Where(q => q.Person1 == q.Person2)
                    .ToIEnumerableAsync(Executor));
        }

        [Test]
        public async Task AnonymousObjects_Simple()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<JoinedQueryClass>()
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
            var data = await Sql.Query.Sqlite<JoinedQueryClass>()
                .From(result => result.ThePerson)
                .LeftJoin<PersonClass>(result => result.PersonClasses)
                    .On((r, pc) => r.ThePerson.Id == pc.PersonId)
                .LeftJoin<Class>(result => result.Classes)
                    .On((r, pc) => r.PersonClasses.One().ClassId == pc.Id)
                .LeftJoin<ClassTag>(result => result.ClassTags)
                    .On((r, pc) => r.Classes.One().Id == pc.ClassId)
                .LeftJoin<Tag>(result => result.Tags)
                    .On((r, pc) => r.ClassTags.One().TagId == pc.Id)
                .Where(result => result.ThePerson.Id == Data.People.John.Id)
                .Map(q => new
                {
                    name = q.ThePerson.Name,
                    person = q.ThePerson,
                    classes = q.Classes
                        .Select(c => new
                        {
                            className = c.Name,
                            tags = q.Tags
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
        public async Task ValueTuplesInQueryFirstPart()
        {
            // arrange
            // act
            var data = await Sql.Query.Sqlite<(Person person, PersonClass[] personClasses, Class[] classes)>()
                .From(result => result.person)
                .LeftJoin<PersonClass>(result => result.personClasses)
                    .On((r, pc) => r.person.Id == pc.PersonId)
                .LeftJoin<Class>(result => result.classes)
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
            var data = await Sql.Query.Sqlite<Person>()
                .From()
                .Map(x => new Person(x.Id, x.Name, x.Gender))
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
            var data = await Sql.Query.Sqlite<Person>()
                .From()
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
            var data = await Sql.Query.Sqlite<QueryClass1>()
                .From(x => x.Person)
                .Map(x => new ObjectWithConstructorArgs2Test(x.Person))
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
            var data = await Sql.Query.Sqlite<PersonsData>()
                .From()
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
            var data = await Sql.Query.Sqlite<PersonsData>()
                .From()
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
            var data = await Sql.Query.Sqlite<Person>()
                .From()
                .Map(x => new ObjectWithConstructorArgs6Test
                {
                    PersonWithName = new Person(0, x.Name, 0),
                    PersonWithGender = new Person(0, null, x.Gender),
                    Inner = new ObjectWithConstructorArgs6Test
                    {
                        PersonWithName = new Person(0, x.Name, 0),
                        PersonWithGender = new Person(0, null, x.Gender),
                    }
                })
                .ToIEnumerableAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count());
            
            Assert.AreEqual(new Person(0, "John", 0), data.First().PersonWithName);
            Assert.AreEqual(new Person(0, null, Gender.Male), data.First().PersonWithGender);
            Assert.AreEqual(new Person(0, "John", 0), data.First().Inner.PersonWithName);
            Assert.AreEqual(new Person(0, null, Gender.Male), data.First().Inner.PersonWithGender);
            
            Assert.AreEqual(new Person(0, "Mary", 0), data.ElementAt(1).PersonWithName);
            Assert.AreEqual(new Person(0, null, Gender.Female), data.ElementAt(1).PersonWithGender);
            Assert.AreEqual(new Person(0, "Mary", 0), data.ElementAt(1).Inner.PersonWithName);
            Assert.AreEqual(new Person(0, null, Gender.Female), data.ElementAt(1).Inner.PersonWithGender);
        }

        class ObjectWithConstructorArgs_OuterSelectTest
        {
            public readonly IEnumerable<Class> Classes;

            public ObjectWithConstructorArgs_OuterSelectTest(IEnumerable<Class> classes)
            {
                Classes = classes;
            }
        }

        [Test]
        public async Task ObjectWithConstructorArgs_OuterSelect()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Map(x => new
                {
                    personName = x.ThePerson.Name,
                    classes = new ObjectWithConstructorArgs_OuterSelectTest(x.Classes)
                })
                .ToIEnumerableAsync(Executor);

            // assert
            Assert.AreEqual(2, data.Count());
            
            Assert.AreEqual(Data.People.John.Name, data.First().personName);
            CollectionAssert.AreEqual(new [] { Data.Classes.Tennis, Data.Classes.Archery }, data.First().classes.Classes);
            
            Assert.AreEqual(Data.People.Mary.Name, data.ElementAt(1).personName);
            CollectionAssert.AreEqual(new [] { Data.Classes.Tennis }, data.ElementAt(1).classes.Classes);
        }

        class ObjectWithConstructorArgs_InnerSelectTest : EqComparer
        {
            public readonly Class Class;

            public ObjectWithConstructorArgs_InnerSelectTest(Class theClass)
            {
                Class = theClass;
            }

            public override int GetHashCode() => Class.GetHashCode();
            public override bool Equals(object p)
            {
                var cls = (p as ObjectWithConstructorArgs_InnerSelectTest)?.Class;
                return cls == Class;
            }
        }

        [Test]
        public async Task ObjectWithConstructorArgs_InnerSelect()
        {
            // arrange
            // act
            var data = await FullyJoinedQuery()
                .Map(x => new
                {
                    personName = x.ThePerson.Name,
                    classes = x.Classes
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
    }
}
