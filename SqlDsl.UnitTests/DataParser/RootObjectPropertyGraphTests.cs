using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using SqlDsl.UnitTests.FullPathTests.Environment;
using SqlDsl.DataParser;
using SqlDsl.Mapper;
using SqlDsl.Schema;
using SqlDsl.UnitTests.FullPathTests;

namespace SqlDsl.UnitTests.DataParser
{
    [TestFixture]
    public class RootObjectPropertyGraphTests
    {
        /// <summary>
        /// The value is not important. Needed to initialze a query builder
        /// </summary>
        public static readonly SqlType TestFlavour = SqlType.Sqlite;

        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public List<PersonClass> PersonClasses { get; set; }
            public List<Class> Classes { get; set; }
            public List<ClassTag> ClassTags { get; set; }
            public List<Tag> Tags { get; set; }
            public List<Purchase> PurchasesByClass { get; set; }
        }

        class DontCompareType { }

        void CompareAndDisplayAllObjsOnFailure(ObjectPropertyGraph expected, ObjectPropertyGraph actual)
        {
            try
            {
                Compare(expected, actual);
            }
            catch (AssertionException)
            {
                Assert.Fail($"Objects are not equal:\n\nexpected:\n{expected}\n\nactual:\n{actual}\n");
            }
        }

        void Compare(ObjectPropertyGraph expected, ObjectPropertyGraph actual)
        {
            if (expected == null && actual == null) return;
            if (expected == null || actual == null) Fail("Actual or expected are null");

            if (expected.ObjectType != typeof(DontCompareType))
                Assert.AreEqual(expected.ObjectType, actual.ObjectType);

            ComparePrimaryKeyColumns(expected, actual);

            if (expected.SimpleProps.Count() != actual.SimpleProps.Count()) Fail("Simple props count");
            for (var i = 0; i < expected.SimpleProps.Count(); i++)
            {
                var x_ = expected.SimpleProps.ElementAt(i);
                var y_ = actual.SimpleProps.ElementAt(i);

                Assert.AreEqual(x_.Index, y_.Index, "Simple prop " + i + " index");
                Assert.AreEqual(x_.Name, y_.Name, "Simple prop " + i + " name");
                Assert.AreEqual(x_.ResultPropertyType, y_.ResultPropertyType, "Simple prop " + i + " resultPropertyType");
                Assert.AreEqual(x_.DataCellType, y_.DataCellType, "Simple prop " + i + " dataCellType");

                CollectionAssert.AreEqual(x_.PrimaryKeyColumns, y_.PrimaryKeyColumns, ErrMessage("Simple prop " + i));
            }

            if (expected.ComplexProps.Count() != actual.ComplexProps.Count()) Fail("Complex props count");
            for (var i = 0; i < expected.ComplexProps.Count(); i++)
            {
                var x_ = expected.ComplexProps.ElementAt(i);
                var y_ = actual.ComplexProps.ElementAt(i);

                if (x_.Name != y_.Name)
                    Fail("Complex prop " + i + " name");

                Compare(x_.Value, y_.Value);
            }

            if (expected.SimpleConstructorArgs.Count() != actual.SimpleConstructorArgs.Count()) Fail("Simple c args count");
            for (var i = 0; i < expected.SimpleConstructorArgs.Count(); i++)
            {
                var x_ = expected.SimpleConstructorArgs.ElementAt(i);
                var y_ = actual.SimpleConstructorArgs.ElementAt(i);
                
                Assert.AreEqual(x_.Index, y_.Index, "Simple cArg " + i + " index");
                Assert.AreEqual(x_.ArgIndex, y_.ArgIndex, "Simple cArg " + i + " argIndex");
                Assert.AreEqual(x_.ResultPropertyType, y_.ResultPropertyType, "Simple cArg " + i + " resultPropertyType");
                Assert.AreEqual(x_.DataCellType, y_.DataCellType, "Simple cArg " + i + " dataCellType");
                CollectionAssert.AreEqual(x_.PrimaryKeyColumns, y_.PrimaryKeyColumns, ErrMessage("Simple cArg " + i));
            }

            if (expected.ComplexConstructorArgs.Count() != actual.ComplexConstructorArgs.Count()) Fail("Complex cArgs count");
            for (var i = 0; i < expected.ComplexConstructorArgs.Count(); i++)
            {
                var x_ = expected.ComplexConstructorArgs.ElementAt(i);
                var y_ = actual.ComplexConstructorArgs.ElementAt(i);
                
                Assert.AreEqual(x_.ArgIndex, y_.ArgIndex, "Complex prop " + i + " index");
                Assert.AreEqual(x_.ConstuctorArgType, y_.ConstuctorArgType, "Complex prop " + i + " constructor arg type");
                Compare(x_.Value, y_.Value);
            }

            string ErrMessage(string message = null) 
            {
                message = message == null ? "" : (" " + message);
                return $"Objects are not equal:{message}\n\nexpected:\n{expected}\n\nactual:\n{actual}\n";
            }

            void Fail(string message = null) 
            {
                Assert.Fail(ErrMessage(message));
            }
        }

        void ComparePrimaryKeyColumns(ObjectPropertyGraph expected, ObjectPropertyGraph actual)
        {
            if (expected == null && actual == null) return;
            if (expected == null || actual == null) Fail();

            if (expected.PrimaryKeyColumns.Count() != actual.PrimaryKeyColumns.Count()) Fail("PrimaryKeyColumns count");
            for (var i = 0; i < expected.PrimaryKeyColumns.Count(); i++)
            {
                if (expected.PrimaryKeyColumns.ElementAt(i) !=
                    actual.PrimaryKeyColumns.ElementAt(i))
                    Fail("PrimaryKeyColumn " + i);
            }

            if (expected.ComplexProps.Count() != actual.ComplexProps.Count()) Fail("Complex props count");
            for (var i = 0; i < expected.ComplexProps.Count(); i++)
            {
                var x_ = expected.ComplexProps.ElementAt(i);
                var y_ = actual.ComplexProps.ElementAt(i);

                if (x_.Name != y_.Name)
                    Fail("Complex prop " + i + " name");

                ComparePrimaryKeyColumns(x_.Value, y_.Value);
            }

            string ErrMessage(string message = null) 
            {
                message = message == null ? "" : (" " + message);
                return $"Objects are not equal:{message}\n\nexpected:\n{expected}\n\nactual:\n{actual}\n";
            }

            void Fail(string message = null) 
            {
                Assert.Fail(ErrMessage(message));
            }
        }

        [Test]
        public void SimplePropertyGraph_WithRootAndJoins_CreatesCorrectObjectPropertyGraph()
        {
            // arrange
            // act
            var actual = Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoinMany<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .BuildObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(JoinedQueryClass),
                null, 
                new[]
                {
                    new ComplexProp("PersonClasses", new ObjectPropertyGraph(
                        typeof(PersonClass),
                        new[]
                        {
                            new SimpleProp(2, "PersonId", new int[0], typeof(long), typeof(long)),
                            new SimpleProp(3, "ClassId", new int[0], typeof(long), typeof(long))
                        }, 
                        null, 
                        new[]{1})),
                    new ComplexProp("ThePerson", new ObjectPropertyGraph(
                        typeof(Person),
                        new[]
                        {
                            new SimpleProp(4, "Id", new int[0], typeof(long), typeof(long)),
                            new SimpleProp(5, "Name", new int[0], typeof(string), typeof(string)),
                            new SimpleProp(6, "Gender", new int[0], typeof(Gender), typeof(Gender)),
                            new SimpleProp(7, "IsMember", new int[0], typeof(bool), typeof(bool))
                        }, 
                        null, 
                        null))
                }, 
                new[] { 0 });

                

            Compare(expected, actual);
        }

        [Test]
        public void PropertyGraph_WithRootAndJoins_CreatesCorrectObjectPropertyGraph()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .BuildObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(QueryContainer),
                null, 
                new[]
                {
                    new ComplexProp("ThePersonsData", new ObjectPropertyGraph(
                        typeof(PersonsData),
                        new[]
                        {
                            new SimpleProp(6, "PersonId", new int[0], typeof(long), typeof(long)),
                            new SimpleProp(7, "Data", new int[0], typeof(byte[]), typeof(byte[]))
                        }, 
                        null, 
                        new[]{1})),
                    new ComplexProp("ThePersonClasses", new ObjectPropertyGraph(
                        typeof(PersonClass),
                        new[]
                        {
                            new SimpleProp(8, "PersonId", new int[0], typeof(long), typeof(long)),
                            new SimpleProp(9, "ClassId", new int[0], typeof(long), typeof(long))
                        }, 
                        null, 
                        new[]{2})),
                    new ComplexProp("TheClasses", new ObjectPropertyGraph(
                        typeof(Class),
                        new[]
                        {
                            new SimpleProp(10, "Id", new int[0], typeof(long), typeof(long)),
                            new SimpleProp(11, "Name", new int[0], typeof(string), typeof(string))
                        }, 
                        null, 
                        new[]{3})),
                    new ComplexProp("TheClassTags", new ObjectPropertyGraph(
                        typeof(ClassTag),
                        new[]
                        {
                            new SimpleProp(12, "ClassId", new int[0], typeof(long), typeof(long)),
                            new SimpleProp(13, "TagId", new int[0], typeof(long), typeof(long))
                        }, 
                        null, 
                        new[]{4})),
                    new ComplexProp("TheTags", new ObjectPropertyGraph(
                        typeof(Tag),
                        new[]
                        {
                            new SimpleProp(14, "Id", new int[0], typeof(long), typeof(long)),
                            new SimpleProp(15, "Name", new int[0], typeof(string), typeof(string))
                        }, 
                        null, 
                        new[]{5})),
                    new ComplexProp("ThePerson", new ObjectPropertyGraph(
                        typeof(Person),
                        new[]
                        {
                            new SimpleProp(16, "Id", new int[0], typeof(long), typeof(long)),
                            new SimpleProp(17, "Name", new int[0], typeof(string), typeof(string)),
                            new SimpleProp(18, "Gender", new int[0], typeof(Gender), typeof(Gender)),
                            new SimpleProp(19, "IsMember", new int[0], typeof(bool), typeof(bool))
                        }, 
                        null, 
                        null))
                }, 
                new[] { 0 });              

            Compare(expected, actual);
        }

        class MappedClass
        {
            public string ClassName;
            public IEnumerable<string> TagNames;
        }

        class MappedVersion 
        {
            public string PersonName;
            public IEnumerable<MappedClass> MappedClasses;
        }

        [Test]
        public void PropertyGraph_WithMapping_CreatesCorrectObjectPropertyGraph()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(x => new MappedVersion
                {
                    PersonName = x.ThePerson.Name
                })
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(MappedVersion),
                new[]
                {
                    new SimpleProp(1, "PersonName", new int[0], typeof(string), typeof(string))
                },
                null, 
                new[] { 0 });
            
            Compare(expected, actual);
        }

        [Test]
        public void PropertyGraph_WithMapping2_CreatesCorrectObjectPropertyGraph()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(x => new MappedVersion
                {
                    PersonName = x.ThePerson.Name,
                    MappedClasses = x.TheClasses
                        .Select(c => new MappedClass
                        {
                            ClassName = c.Name
                        })
                })
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(MappedVersion),
                new[]
                {
                    new SimpleProp(3, "PersonName", new int[0], typeof(string), typeof(string))
                },
                new[]
                {
                    new ComplexProp("MappedClasses", new ObjectPropertyGraph(
                        typeof(MappedClass),
                        new[]
                        {
                            new SimpleProp(4, "ClassName", new int[0], typeof(string), typeof(string))
                        }, 
                        null, 
                        new[]{1, 2}))
                }, 
                new[] { 0 });

            Compare(expected, actual);
        }

        [Test]
        public void PropertyGraph_The_Gold_Standard()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(x => new MappedVersion
                {
                    PersonName = x.ThePerson.Name,
                    MappedClasses = x.TheClasses
                        .Select(c => new MappedClass
                        {
                            ClassName = c.Name,
                            TagNames = x.TheTags
                                .Select(t => t.Name)

                        })
                })
                .BuildMappedObjetPropertyGraph();

            // assert
            // [0-ThePerson.#pk0, 1-TheClasses.#pk0, 2-TheTags.#pk0, 3-ThePersonClasses.#pk0, 4-TheClassTags.#pk0, 5-PersonName, 6-MappedClasses.ClassName, 7-MappedClasses.TagNames]
            var expected = new ObjectPropertyGraph(
                typeof(MappedVersion),
                new[]
                {
                    new SimpleProp(5, "PersonName", new int[0], typeof(string), typeof(string))
                },
                new[]
                {
                    new ComplexProp("MappedClasses", new ObjectPropertyGraph(
                        typeof(MappedClass),
                        new[]
                        {
                            new SimpleProp(6, "ClassName", new int[0], typeof(string), typeof(string)),
                            new SimpleProp(7, "TagNames", new int[]{3,4}, typeof(IEnumerable<string>), typeof(string))
                        }, 
                        null, 
                        new[]{1,2}))
                }, 
                new[] { 0 });

            CompareAndDisplayAllObjsOnFailure(expected, actual);
        }

        class Tag2
        {
            public string TagName;
        }

        class MappedClass2
        {
            public string ClassName;
            public IEnumerable<Tag2> Tags;
        }

        class MappedVersion2
        {
            public string PersonName;
            public IEnumerable<MappedClass2> MappedClasses;
        }

        [Test]
        public void PropertyGraph_WithMapping4_CreatesCorrectObjectPropertyGraph()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(x => new MappedVersion2
                {
                    PersonName = x.ThePerson.Name,
                    MappedClasses = x.TheClasses
                        .Select(c => new MappedClass2
                        {
                            ClassName = c.Name,
                            Tags = x.TheTags
                                .Select(t => new Tag2 { TagName = t.Name })

                        })
                })
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(MappedVersion2),
                new[]
                {
                    new SimpleProp(5, "PersonName", new int[0], typeof(string), typeof(string))
                },
                new[]
                {
                    new ComplexProp("MappedClasses", new ObjectPropertyGraph(
                        typeof(MappedClass2),
                        new[]
                        {
                            new SimpleProp(6, "ClassName", new int[0], typeof(string), typeof(string))
                        }, 
                        new[]
                        {
                            new ComplexProp("Tags", new ObjectPropertyGraph(
                                typeof(Tag2),
                                new[]
                                {
                                    new SimpleProp(7, "TagName", new int[0], typeof(string), typeof(string))
                                }, 
                                null, 
                                new[]{3,4}))
                        }, 
                        new[]{1,2}))
                }, 
                new[] { 0 });

            Compare(expected, actual);
        }

        class DifficultScenario
        {
            public DifficultScenarioInner[] FavouriteClasses;
        }

        class DifficultScenarioInner
        {
            public long[] TagIds;
        }

        [Test]
        public void PropertyGraph_WithMapping_DifficultScenario_CreatesCorrectObjectPropertyGraph()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(query => new DifficultScenario
                { 
                    FavouriteClasses = query.TheClasses
                        .Select(c => new DifficultScenarioInner
                        {
                            TagIds = query.TheClassTags
                                .Select(t => t.TagId)
                                .ToArray()
                        })
                        .ToArray()
                })
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(DifficultScenario),
                null,
                new[]
                {
                    new ComplexProp("FavouriteClasses", new ObjectPropertyGraph(
                        typeof(DifficultScenarioInner),
                        new[]
                        {
                            new SimpleProp(4, "TagIds", new int[]{3}, typeof(long[]), typeof(long))
                        }, 
                        null, 
                        new[]{1,2}))
                }, 
                new[] { 0 });

            Compare(expected, actual);
        }

        class DifficultScenario2
        {
            public long[] TagIds;
        }

        [Test]
        public void PropertyGraph_WithMapping_DifficultScenario_CreatesCorrectObjectPropertyGraph2()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(query => new DifficultScenario2
                { 
                    TagIds = query.TheClassTags
                        .Select(t => t.TagId)
                        .ToArray()
                })
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(DifficultScenario2),
                new []
                {
                    new SimpleProp(4, "TagIds", new int[]{1,2,3}, typeof(long[]), typeof(long))
                },
                null,
                new[] { 0 });

            Compare(expected, actual);
        }

        class DeepJoinedClass
        {
            public DeepJoinedClass Inner;

            public DeepJoinedClassData[] FavouriteClasses;
        }

        class DeepJoinedClassData
        {
            public long[] TagIds;
        }

        [Test]
        public void PropertyGraph_WithMapping_DifficultScenario_CreatesCorrectObjectPropertyGraph3()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(query => new DeepJoinedClass
                { 
                    Inner = new DeepJoinedClass
                    {
                        Inner = new DeepJoinedClass
                        {
                            FavouriteClasses = query.TheClasses
                                .Select(c => new DeepJoinedClassData
                                {
                                    TagIds = query.TheClassTags
                                        .Select(t => t.TagId)
                                        .ToArray()
                                })
                                .ToArray()
                        }
                    }
                })
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(DeepJoinedClass),
                null,
                new []
                {
                    new ComplexProp("Inner", new ObjectPropertyGraph(
                        typeof(DeepJoinedClass),
                        null,
                        new []
                        {
                            new ComplexProp("Inner", new ObjectPropertyGraph
                            (
                                typeof(DeepJoinedClass),
                                null,
                                new []
                                {
                                    new ComplexProp("FavouriteClasses", new ObjectPropertyGraph
                                    (
                                        typeof(DeepJoinedClassData),
                                        new[] 
                                        {
                                            new SimpleProp(4, "TagIds", new int[]{3}, typeof(long[]), typeof(long))
                                        },
                                        null,
                                        new[]{1, 2}
                                    ))
                                },
                                null
                            ))
                        },
                        null
                    ))
                },
                new[] { 0 });

            Compare(expected, actual);
        }

        [Test]
        public void PropertyGraph_JoinOnNonTable_ReturnsCorrectOPG()
        {
            // arrange
            // act
            var actual = Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoinMany<PersonClass>(result => result.PersonClasses)
                    .On((q, c) => c.ClassId == Data.Classes.Tennis.Id)
                .BuildObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(JoinedQueryClass),
                null, 
                new[]
                {
                    new ComplexProp("PersonClasses", new ObjectPropertyGraph(
                        typeof(PersonClass),
                        new[]
                        {
                            new SimpleProp(2, "PersonId", new int[0], typeof(long), typeof(long)),
                            new SimpleProp(3, "ClassId", new int[0], typeof(long), typeof(long))
                        }, 
                        null, 
                        new[]{1})),
                    new ComplexProp("ThePerson", new ObjectPropertyGraph(
                        typeof(Person),
                        new[]
                        {
                            new SimpleProp(4, "Id", new int[0], typeof(long), typeof(long)),
                            new SimpleProp(5, "Name", new int[0], typeof(string), typeof(string)),
                            new SimpleProp(6, "Gender", new int[0], typeof(Gender), typeof(Gender)),
                            new SimpleProp(7, "IsMember", new int[0], typeof(bool), typeof(bool))
                        }, 
                        null, 
                        null))
                }, 
                new[] { 0 });

            Compare(expected, actual);
        }

        class DataCellTypeIsArray1
        {
            // warning CS0649: Field 'Person/PersonsData' is never assigned to, and will always have its default value null
            #pragma warning disable 0649
            public Person Person;
            public PersonsData PersonsData;
            public IEnumerable<PersonClass> PersonClasses;
            #pragma warning restore 0649
        }

        class DataCellTypeIsArray1Result
        {
            public long[] TheClassIds;
            public byte[] TheData;
        }

        [Test]
        public void PropertyGraph_DataCellTypeIsArray_ReturnsCorrectOPG1()
        {
            // arrange
            // act
            var actual = Sql.Query.Sqlite<DataCellTypeIsArray1>()
                .From<Person>(x => x.Person)
                .InnerJoinMany<PersonClass>(result => result.PersonClasses)
                    .On((q, c) => c.PersonId == q.Person.Id)
                .InnerJoinOne<PersonsData>(result => result.PersonsData)
                    .On((q, c) => c.PersonId == q.Person.Id)
                .Map(x => new DataCellTypeIsArray1Result
                {
                    TheData = x.PersonsData.Data,
                    TheClassIds = x.PersonClasses
                        .Select(c => c.ClassId)
                        .ToArray()
                })
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(DataCellTypeIsArray1Result),
                new[]
                {
                    new SimpleProp(3, "TheData", new [] {1}, typeof(byte[]), typeof(byte[])),
                    new SimpleProp(4, "TheClassIds", new [] {2}, typeof(long[]), typeof(long))
                }, 
                null, 
                new[] { 0 });

            Compare(expected, actual);
        }

        class DataCellTypeIsArray2
        {
            // warning CS0649: Field 'PersonClasses/PersonsData' is never assigned to, and will always have its default value null
            #pragma warning disable 0649
            public Person Person;
            public IEnumerable<PersonsData> PersonsData;
            public IEnumerable<PersonClass> PersonClasses;
            #pragma warning restore 0649
        }

        class DataCellTypeIsArray2Result
        {
            public long[] ClassIds;
            public byte[][] Data;
        }

        [Test]
        public void PropertyGraph_DataCellTypeIsArray_ReturnsCorrectOPG2()
        {
            // arrange
            // act
            var actual = Sql.Query.Sqlite<DataCellTypeIsArray2>()
                .From<Person>(x => x.Person)
                .InnerJoinMany<PersonClass>(result => result.PersonClasses)
                    .On((q, c) => c.PersonId == q.Person.Id)
                .InnerJoinMany<PersonsData>(result => result.PersonsData)
                    .On((q, c) => c.PersonId == q.Person.Id)
                .Map(x => new DataCellTypeIsArray2Result
                {
                    Data = x.PersonsData
                        .Select(c => c.Data)
                        .ToArray(),
                    ClassIds = x.PersonClasses
                        .Select(c => c.ClassId)
                        .ToArray()
                })
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(DataCellTypeIsArray2Result),
                new[]
                {
                    new SimpleProp(3, "Data", new [] {1}, typeof(byte[][]), typeof(byte[])),
                    new SimpleProp(4, "ClassIds", new [] {2}, typeof(long[]), typeof(long))
                }, 
                null, 
                new[] { 0 });

            Compare(expected, actual);
        }

        class SemiPerson
        {
            public readonly string Name;
            public readonly Gender Gender;

            public SemiPerson(Person person, Gender gender)
            {
                Name = person.Name;
                Gender = gender;
            }
        }

        [Test]
        public void PropertyGraph_MappedValHasConstructorArgs_ReturnsCorrectOPG()
        {
            // arrange
            // act
            var actual = Sql.Query.Sqlite<Person>()
                .Map(x => new SemiPerson(x, x.Gender))
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(SemiPerson),
                new[]
                {
                    new SimpleProp(0, "#pk0", new int[0], (Type)null, (Type)null)
                }, 
                null, 
                new[] { 0 },
                simpleConstructorArgs: new[]
                {
                    new SimpleConstructorArg(5, 1, new int[0], typeof(Gender), typeof(Gender))
                },
                complexConstructorArgs: new [] 
                {
                    new ComplexConstructorArg(0, typeof(Person), new ObjectPropertyGraph(
                        typeof(Person),
                        new[]
                        {
                            new SimpleProp(1, "Id", new int[0], typeof(long), typeof(long)),
                            new SimpleProp(2, "Name", new int[0], typeof(string), typeof(string)),
                            new SimpleProp(3, "Gender", new int[0], typeof(Gender), typeof(Gender)),
                            new SimpleProp(4, "IsMember", new int[0], typeof(bool), typeof(bool))
                        }, 
                        null, 
                        null))
                });

            Compare(expected, actual);
        }

        class Inner
        {
            public Class[] classes2;

            public Inner(Class[] classes2)
            {
                this.classes2 = classes2;
            }

            public Inner()
            {
            }
        }

        class Outer
        {
            public Inner[] classes1;

            public Outer(Inner[] classes1)
            {
                this.classes1 = classes1;
            }

            public Outer()
            {
            }
        }

        [Test]
        public void PropertyGraph_WithMultiLevelProperties_ReturnsCorrectOPG()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(q => new Outer
                {
                    classes1 = q.ThePersonClasses
                        .Select(pc => new Inner
                        {
                            classes2 = q.TheClasses.ToArray()
                        })
                        .ToArray()
                })
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(Outer),
                null, 
                new[]
                {
                    new ComplexProp("classes1", new ObjectPropertyGraph(
                        typeof(Inner),
                        null,
                        new[] 
                        {
                            new ComplexProp("classes2", new ObjectPropertyGraph(
                                typeof(Class),
                                new[] 
                                {
                                    new SimpleProp(3, "Id", new int[0], typeof(long), typeof(long)),
                                    new SimpleProp(4, "Name", new int[0], typeof(string), typeof(string))
                                },
                                null,
                                new[]{2}
                            ))
                        },
                        new[]{1}
                    ))
                }, 
                new[] { 0 });

            Compare(expected, actual);
        }

        [Test]
        public void PropertyGraph_WithMultiLevelConstructorArgs_ReturnsCorrectOPG()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(q => new Outer(q.ThePersonClasses
                    .Select(pc => new Inner(q.TheClasses.ToArray()))
                    .ToArray()))
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(Outer),
                null,
                null, 
                new[] { 0 },
                complexConstructorArgs: new[]
                {
                    new ComplexConstructorArg(0, typeof(Inner[]), new ObjectPropertyGraph(
                        typeof(Inner),
                        null,
                        null,
                        new[]{1},
                        complexConstructorArgs: new[] 
                        {
                            new ComplexConstructorArg(0, typeof(Class[]), new ObjectPropertyGraph(
                                typeof(Class),
                                new[] 
                                {
                                    new SimpleProp(3, "Id", new int[0], typeof(long), typeof(long)),
                                    new SimpleProp(4, "Name", new int[0], typeof(string), typeof(string))
                                },
                                null,
                                new[]{2}
                            ))
                        }
                    ))
                });

            Compare(expected, actual);
        }

        class AnotherDifficultCase
        {
            public long ClassId;
        }

        [Test]
        public void PropertyGraph_ReturnsMultipleComplexArgs_ReturnsCorrectOPG1()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(p => p.ThePersonClasses.Select(pc => new AnotherDifficultCase { ClassId = pc.ClassId }).ToList())
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(PropMapValue<List<AnotherDifficultCase>>),
                null,
                new []
                {
                    new ComplexProp("Value", new ObjectPropertyGraph(
                        typeof(AnotherDifficultCase),
                        new []
                        {
                            new SimpleProp(2, "ClassId", new int[0], typeof(long), typeof(long)),   
                        },
                        null, 
                        new int[] { 1 }))
                }, 
                new[] { 0 });

            Compare(expected, actual);
        }

        [Test]
        public void PropertyGraph_ReturnsOneComplexArgs_ReturnsCorrectOPG()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(p => p.ThePerson)
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(PropMapValue<Person>),
                null,
                new []
                {
                    new ComplexProp("Value", new ObjectPropertyGraph(
                        typeof(Person),
                        new []
                        {
                            new SimpleProp(1, "Id", new int[0], typeof(long), typeof(long)),
                            new SimpleProp(2, "Name", new int[0], typeof(string), typeof(string)),
                            new SimpleProp(3, "Gender", new int[0], typeof(Gender), typeof(Gender)),
                            new SimpleProp(4, "IsMember", new int[0], typeof(bool), typeof(bool))
                        },
                        null, 
                        new int[0]))
                }, 
                new[] { 0 });

            Compare(expected, actual);
        }

        [Test]
        public void PropertyGraph_ReturnsMultipleComplexArgs_ReturnsCorrectOPG()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(p => p.ThePersonClasses.ToList())
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(PropMapValue<List<PersonClass>>),
                null,
                new []
                {
                    new ComplexProp("Value", new ObjectPropertyGraph(
                        typeof(PersonClass),
                        new []
                        {
                            new SimpleProp(2, "PersonId", new int[0], typeof(long), typeof(long)),   
                            new SimpleProp(3, "ClassId", new int[0], typeof(long), typeof(long))   
                        },
                        null, 
                        new[] { 1 }))
                }, 
                new[] { 0 });

            CompareAndDisplayAllObjsOnFailure(expected, actual);
        }

        [Test]
        public void PropertyGraph_ReturnsOneSimpleArg_ReturnsCorrectOPG()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(p => p.ThePerson.Name)
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(PropMapValue<string>),
                new []
                {
                    new SimpleProp(1, "Value", new int[0], typeof(string), typeof(string))
                },
                null, 
                new[] { 0 });

            CompareAndDisplayAllObjsOnFailure(expected, actual);
        }

        [Test]
        public void PropertyGraph_ReturnsMultipleSimpleArg_ReturnsCorrectOPG()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(p => p.TheClasses.Select(c => c.Name))
                .BuildMappedObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(PropMapValue<IEnumerable<string>>),
                new []
                {
                    new SimpleProp(3, "Value", new[] { 1, 2 }, typeof(IEnumerable<string>), typeof(string))
                },
                null, 
                new[] { 0 });

            CompareAndDisplayAllObjsOnFailure(expected, actual);
        }

        class CountAndGroupClass
        {
            public string person;
            public int classes;
        }

        [Test]
        public void PropertyGraph_WithCountAndGroup_ReturnsCorrectOPG()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(p => new CountAndGroupClass
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Count()
                })
                .BuildMappedObjetPropertyGraph();
                
            // assert
            var expected = new ObjectPropertyGraph(
                typeof(CountAndGroupClass),
                new []
                {
                    new SimpleProp(1, "person", new int[0], typeof(string), typeof(string)),   
                    new SimpleProp(2, "classes", new int[0], typeof(int), typeof(int))   
                },
                null, 
                new[] { 0 });

            Compare(expected, actual);
        }

        [Test]
        public void PropertyGraph_WithCountAndGroup_ReturnsCorrectOPG2()
        {
            // arrange
            // act
            var actual = TestUtils
                .FullyJoinedQuery(TestFlavour)
                .Map(p => new CountAndGroupClass
                {
                    person = p.ThePerson.Name,
                    classes = p.TheClasses.Select(x => x.Id).Count()
                })
                .BuildMappedObjetPropertyGraph();
                
            // assert
            var expected = new ObjectPropertyGraph(
                typeof(CountAndGroupClass),
                new []
                {
                    new SimpleProp(1, "person", new int[0], typeof(string), typeof(string)),   
                    new SimpleProp(2, "classes", new int[0], typeof(int), typeof(int))   
                },
                null, 
                new[] { 0 });

            Compare(expected, actual);
        }
    
        [Table("Person")]
        public class PersonWithAttributes
        {
            [Column("Id")]
            public long TheId { get; set; }
            
            [Column("Name")]
            public string TheName { get; set; }
        }

        [Test]
        public void PropertyGraph_WithColumnAttribute_ReturnsCorrectOPG()
        {
            // arrange
            // act
            var actual = Sql.Query
                .Sqlite<PersonWithAttributes>()
                .BuildObjetPropertyGraph();
                
            // assert
            var expected = new ObjectPropertyGraph(
                typeof(PersonWithAttributes),
                new []
                {
                    new SimpleProp(0, "#pk0", new int[0], (Type)null, (Type)null),
                    new SimpleProp(1, "TheId", new int[0], typeof(long), typeof(long)),   
                    new SimpleProp(2, "TheName", new int[0], typeof(string), typeof(string))   
                },
                null, 
                new[] { 0 });

            Compare(expected, actual);
        }
        

        class SimpleMapOn1Table_WithMultipleResultsResult
        {
            public string ThePerson;
            public SimpleMapOn1Table_WithMultipleResultsClassResult[] TheClasses;
        }

        class SimpleMapOn1Table_WithMultipleResultsClassResult
        {
            public string Name;
            public float[] Prices;
        }

        [Test]
        public void SimpleMapOn1Table_WithMultipleResults_AndProperties()
        {
            // arrange
            // act
            var actual = Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .LeftJoinMany<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .LeftJoinMany<Class>(q => q.Classes)
                    .On((q, c) => q.PersonClasses.One().ClassId == c.Id)
                .LeftJoinMany<ClassTag>(q => q.ClassTags)
                    .On((q, ct) => q.Classes.One().Id == ct.ClassId)
                .LeftJoinMany<Tag>(q => q.Tags)
                    .On((q, t) => q.ClassTags.One().TagId == t.Id)
                .LeftJoinMany<Purchase>(q => q.PurchasesByClass)
                    .On((q, t) => q.ThePerson.Id == t.PersonId && q.ThePerson.Id == t.PurchaedForPersonId && q.Classes.One().Id == t.ClassId)
                .Where(x => x.ThePerson.Id == Data.People.Mary.Id)
                .Map(p => new SimpleMapOn1Table_WithMultipleResultsResult
                { 
                    ThePerson = p.ThePerson.Name,
                    TheClasses = p.Classes
                        .Select(c => new SimpleMapOn1Table_WithMultipleResultsClassResult
                        {
                            Name = c.Name,
                            Prices = p.PurchasesByClass.Select(x => x.Amount).ToArray()
                        })
                        .ToArray()
                })
                .BuildMappedObjetPropertyGraph();

            // assert
            // [0-ThePerson.#pk0, 1-Classes.#pk0, 2-PurchasesByClass.#pk0, 3-PersonClasses.#pk0, 4-ThePerson, 5-TheClasses.Name, 6-TheClasses.Prices]
            var expected = new ObjectPropertyGraph(
                typeof(SimpleMapOn1Table_WithMultipleResultsResult),
                new []
                {
                    new SimpleProp(4, "ThePerson", new int[0], typeof(string), typeof(string))
                },
                new []
                {
                    new ComplexProp("TheClasses", new ObjectPropertyGraph(
                        typeof(SimpleMapOn1Table_WithMultipleResultsClassResult),
                        new []
                        {
                            new SimpleProp(5, "Name", new int[0], typeof(string), typeof(string)),   
                            new SimpleProp(6, "Prices", new int[] { 3 }, typeof(float[]), typeof(float))   
                        },
                        null, 
                        new[] { 1 ,2 }))
                }, 
                new[] { 0 });

            CompareAndDisplayAllObjsOnFailure(expected, actual);
        }

        class ClassesByTag
        {
            // warning CS0649: Field ... is never assigned to, and will always have its default value null
            #pragma warning disable 0649
            public Tag TheTag;
            public IEnumerable<Class> TheClasses;
            public IEnumerable<ClassTag> TheClassTags;
            #pragma warning restore 0649
        }

        class ClassesByTagResult
        {
            public string name;
        }

        [Test]
        public void SimpleMapOn1Table_MappedToEnumerableWrappedInObject_ReturnsCorrectOPG()
        {
            // this is a smoke test for SimpleMapOn1Table_MappedToEnumerable_ReturnsCorrectOPG
            // it is essentially the same query but with each result wrapped in an anonymous object

            // arrange
            // act
            var actual = Sql.Query.Sqlite<ClassesByTag>()
                .From(x => x.TheTag)
                .LeftJoinMany(q => q.TheClassTags)
                    .On((q, ct) => q.TheTag.Id == ct.TagId)
                .LeftJoinMany(q => q.TheClasses)
                    .On((q, ct) => q.TheClassTags.One().ClassId == ct.Id)
                .Map(t => new { y = t.TheClasses
                    .Select(c => new ClassesByTagResult { name = t.TheTag.Name }) })
                .BuildMappedObjetPropertyGraph();

            // assert
            // [0-TheTag.#pk0, 1-TheClasses.#pk0, 2-TheClassTags.#pk0, 3-name]
            var expected = new ObjectPropertyGraph(
                typeof(DontCompareType),
                null,
                null, 
                new[] { 0 },
                complexConstructorArgs: new []
                {
                    new ComplexConstructorArg(0, typeof(IEnumerable<ClassesByTagResult>), new ObjectPropertyGraph(
                        typeof(ClassesByTagResult),
                        new []
                        {
                            new SimpleProp(3, "name", new int[0], typeof(string), typeof(string))
                        },
                        null, 
                        new[] { 1, 2 }))
                });

            CompareAndDisplayAllObjsOnFailure(expected, actual);
        }

        [Test]
        public void SimpleMapOn1Table_MappedToEnumerable_ReturnsCorrectOPG()
        {
            // arrange
            // act
            var actual = Sql.Query.Sqlite<ClassesByTag>()
                .From(x => x.TheTag)
                .LeftJoinMany(q => q.TheClassTags)
                    .On((q, ct) => q.TheTag.Id == ct.TagId)
                .LeftJoinMany(q => q.TheClasses)
                    .On((q, ct) => q.TheClassTags.One().ClassId == ct.Id)
                .Map(t => t.TheClasses
                    .Select(c => new ClassesByTagResult { name = t.TheTag.Name }))
                .BuildMappedObjetPropertyGraph();

            // assert
            // [0-TheTag.#pk0, 1-TheClasses.#pk0, 2-TheClassTags.#pk0, 3-name]
            var expected = new ObjectPropertyGraph(
                typeof(PropMapValue<IEnumerable<ClassesByTagResult>>),
                null,
                new []
                {
                    new ComplexProp("Value", new ObjectPropertyGraph(
                        typeof(ClassesByTagResult),
                        new []
                        {
                            new SimpleProp(3, "name", new int[0], typeof(string), typeof(string))
                        },
                        null, 
                        new[] { 1, 2 }))
                }, 
                new int[] { 0 });

            CompareAndDisplayAllObjsOnFailure(expected, actual);
        }

        class myobj<T>
        {
            public T Value { get; set; }
        }

        [Test]
        public void MappedQuery_LeftJoinReturnsNullAndMappedToEnumerable_ReturnsCorrectOPG()
        {
            // arrange
            // act
            var actual = Sql.Query.Sqlite<ClassesByTag>()
                .From(x => x.TheTag)
                .LeftJoinMany(q => q.TheClassTags)
                    .On((q, ct) => q.TheTag.Id == ct.TagId)
                .LeftJoinMany(q => q.TheClasses)
                    .On((q, ct) => q.TheClassTags.One().ClassId == ct.Id)
                .Map(t => t.TheClasses
                    .Select(c => t.TheTag.Name))
                .BuildMappedObjetPropertyGraph();

            // assert
            // [0-TheTag.#pk0, 1-TheClasses.#pk0, 2-TheClassTags.#pk0, 3-Value]
            var expected = new ObjectPropertyGraph(
                typeof(PropMapValue<IEnumerable<string>>),
                new []
                {
                    new SimpleProp(3, "Value", new int[] { 1, 2 }, typeof(IEnumerable<string>), typeof(string))
                },
                null, 
                new[] { 0 });

            // assert
            CompareAndDisplayAllObjsOnFailure(expected, actual);
        }

        [Test]
        public void SqlCase_WithPlainExpressions_ReturnsCorrectOPG()
        {
            // arrange
            // act
            var actual = Sql.Query.Sqlite<ClassesByTag>()
                .From(x => x.TheTag)
                .LeftJoinMany(q => q.TheClassTags)
                    .On((q, ct) => q.TheTag.Id == ct.TagId)
                .LeftJoinMany(q => q.TheClasses)
                    .On((q, ct) => q.TheClassTags.One().ClassId == ct.Id)
                .Map(t => t.TheClasses
                    .Select(c => new
                    {
                        n = t.TheTag.Name,
                        r = t.TheTag.Name == Data.Tags.Sport.Name
                            ? c.Name == Data.Classes.Tennis.Name
                                ? 1
                                : 10
                            : t.TheTag.Name == Data.Tags.BallSport.Name
                                ? 100
                                : 1000
                    }))
                .BuildMappedObjetPropertyGraph();

            // assert
            // [0-TheTag.#pk0, 1-TheClasses.#pk0, 2-TheClassTags.#pk0, 3-Value.#ca0, 4-Value.#ca1]
            var expected = new ObjectPropertyGraph(
                typeof(DontCompareType),
                null,
                new []
                {
                    new ComplexProp("Value", new ObjectPropertyGraph(
                        typeof(DontCompareType),
                        null,
                        null, 
                        new[] { 1, 2 },
                        simpleConstructorArgs: new []
                        {
                            new SimpleConstructorArg(3, 0, new int[0], typeof(string), typeof(string)),
                            new SimpleConstructorArg(4, 1, new int[0], typeof(int), typeof(int))
                        }))
                }, 
                new[] { 0 });
                    

            // assert
            CompareAndDisplayAllObjsOnFailure(expected, actual);
        }

        [Test]
        public void MappedQuery_LeftJoinReturnsNull_ReturnsCorrectElements()
        {
            // arrange
            // act
            var actual = Sql.Query.Sqlite<QueryContainer>()
                .From<Person>(x => x.ThePerson)
                .LeftJoinMany<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.ThePerson.Id == ct.ClassId)
                .Map(x => new
                {
                    thePerson = x.ThePerson.Name,
                    theClassTagIds = x.TheClassTags
                        .Select(y => y.ClassId)
                        .ToArray()
                })
                .BuildMappedObjetPropertyGraph();

            // assert
            // [0-ThePerson.#pk0, 1-TheClassTags.#pk0, 2-#ca0, 3-#ca1]
            var expected = new ObjectPropertyGraph(
                typeof(DontCompareType),
                null,
                null, 
                new [] { 0 },
                simpleConstructorArgs: new[]
                {
                    new SimpleConstructorArg(2, 0, new int[0], typeof(string), typeof(string)),
                    new SimpleConstructorArg(3, 1, new [] { 1 }, typeof(long[]), typeof(long))
                });

            CompareAndDisplayAllObjsOnFailure(expected, actual);
        }

        // See todo in ComplexMapBuilder.BuildMapForConstructor

        // class PreMapped 
        // {
        //     public PreMapped AnotherPreMapped;

        //     public PreMapped(){}

        //     public PreMapped(PreMapped anotherPreMapped) { AnotherPreMapped = anotherPreMapped; }
        // }

        // [Test]
        // public void PropertyGraph_ReturnsMultipleComplexArgsWithNoSimpleProps_ReturnsCorrectOPG1()
        // {
        //     // arrange
        //     // act
        //     var actual = FullyJoinedQuery()
        //         .Map(p => p.PersonClasses.Select(pc => new PreMapped { AnotherPreMapped = new PreMapped() }).ToList())
        //         .BuildObjetPropertyGraph<List<PreMapped>, JoinedQueryClass>();

        //     // assert
        //     var expected = new ObjectPropertyGraph(
        //         typeof(List<PreMapped>),
        //         null,
        //         new []
        //         {
        //             new ComplexProp("AnotherPreMapped", new ObjectPropertyGraph(
        //                 typeof(List<PreMapped>),
        //                 null,
        //                 null, 
        //                 new[] { 0 })
        //             )
        //         },
        //         new[] { 0 });

        //     Compare(expected, actual);
        // }

        // [Test]
        // public void PropertyGraph_ReturnsMultipleComplexArgsWithNoSimpleProps_ReturnsCorrectOPG2()
        // {
        //     // arrange
        //     // act
        //     var actual = FullyJoinedQuery()
        //         .Map(p => p.PersonClasses.Select(pc => new PreMapped(new PreMapped())).ToList())
        //         .BuildObjetPropertyGraph<List<PreMapped>, JoinedQueryClass>();

        //     // The expected needs a bit of work before this test can be used

        //     // assert
        //     var expected = new ObjectPropertyGraph(
        //         typeof(List<PreMapped>),
        //         null,
        //         null,
        //         new[] { 0 },
        //         complexConstructorArgs: new []
        //         {
        //             (0, typeof(PreMapped), new ObjectPropertyGraph(
        //                 typeof(List<PreMapped>),
        //                 null,
        //                 null, 
        //                 new[] { 0 })
        //             )
        //         });

        //     Compare(expected, actual);
        // }
    }
}