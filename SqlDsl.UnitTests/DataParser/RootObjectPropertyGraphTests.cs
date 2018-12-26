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
using SqlDsl.DataParser;
using SqlDsl.Query;
using SqlDsl.Mapper;

namespace SqlDsl.UnitTests.DataParser
{
    [Ignore("Tests results will change as cols are added/removed from query. Remove this attribute when GROUP BY is complete")]
    [TestFixture]
    public class RootObjectPropertyGraphTests
    {
        class JoinedQueryClass
        {
            public Person ThePerson { get; set; }
            public List<PersonClass> PersonClasses { get; set; }
            public List<Class> Classes { get; set; }
            public List<ClassTag> ClassTags { get; set; }
            public List<Tag> Tags { get; set; }
        }

        static QueryBuilder<Sqlite.SqliteSyntax, object, JoinedQueryClass> FullyJoinedQuery()
        {
            return Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<Class>(q => q.Classes)
                    .On((q, c) => q.PersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.ClassTags)
                    .On((q, ct) => q.Classes.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.Tags)
                    .On((q, t) => q.ClassTags.One().TagId == t.Id) as QueryBuilder<Sqlite.SqliteSyntax, object, JoinedQueryClass>;
        }

        void Compare(ObjectPropertyGraph expected, ObjectPropertyGraph actual)
        {
            if (expected == null && actual == null) return;
            if (expected == null || actual == null) Fail();

            Assert.AreEqual(expected.ObjectType, actual.ObjectType);

            CompareRowIdColumnNumbers(expected, actual);

            if (expected.SimpleProps.Count() != actual.SimpleProps.Count()) Fail("Simple props count");
            for (var i = 0; i < expected.SimpleProps.Count(); i++)
            {
                var x_ = expected.SimpleProps.ElementAt(i);
                var y_ = actual.SimpleProps.ElementAt(i);

                Assert.AreEqual(x_.index, y_.index, "Simple prop " + i + " index");
                Assert.AreEqual(x_.name, y_.name, "Simple prop " + i + " name");
                Assert.AreEqual(x_.resultPropertyType, y_.resultPropertyType, "Simple prop " + i + " resultPropertyType");
                Assert.AreEqual(x_.dataCellType, y_.dataCellType, "Simple prop " + i + " dataCellType");

                CollectionAssert.AreEqual(x_.rowNumberColumnIds, y_.rowNumberColumnIds, ErrMessage("Simple prop " + i));
            }

            if (expected.ComplexProps.Count() != actual.ComplexProps.Count()) Fail("Complex props count");
            for (var i = 0; i < expected.ComplexProps.Count(); i++)
            {
                var x_ = expected.ComplexProps.ElementAt(i);
                var y_ = actual.ComplexProps.ElementAt(i);

                if (x_.name != y_.name)
                    Fail("Complex prop " + i);

                Compare(x_.value, y_.value);
            }

            if (expected.SimpleConstructorArgs.Count() != actual.SimpleConstructorArgs.Count()) Fail("Simple c args count");
            for (var i = 0; i < expected.SimpleConstructorArgs.Count(); i++)
            {
                var x_ = expected.SimpleConstructorArgs.ElementAt(i);
                var y_ = actual.SimpleConstructorArgs.ElementAt(i);
                
                Assert.AreEqual(x_.index, y_.index, "Simple cArg " + i + " index");
                Assert.AreEqual(x_.argIndex, y_.argIndex, "Simple cArg " + i + " argIndex");
                Assert.AreEqual(x_.resultPropertyType, y_.resultPropertyType, "Simple cArg " + i + " resultPropertyType");
                Assert.AreEqual(x_.dataCellType, y_.dataCellType, "Simple cArg " + i + " dataCellType");
                CollectionAssert.AreEqual(x_.rowNumberColumnIds, y_.rowNumberColumnIds, ErrMessage("Simple cArg " + i));
            }

            if (expected.ComplexConstructorArgs.Count() != actual.ComplexConstructorArgs.Count()) Fail("Complex cArgs count");
            for (var i = 0; i < expected.ComplexConstructorArgs.Count(); i++)
            {
                var x_ = expected.ComplexConstructorArgs.ElementAt(i);
                var y_ = actual.ComplexConstructorArgs.ElementAt(i);
                
                Assert.AreEqual(x_.argIndex, y_.argIndex, "Complex prop " + i);
                Assert.AreEqual(x_.constuctorArgType, y_.constuctorArgType, "Complex prop " + i);
                Compare(x_.value, y_.value);
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

        void CompareRowIdColumnNumbers(ObjectPropertyGraph expected, ObjectPropertyGraph actual)
        {
            if (expected == null && actual == null) return;
            if (expected == null || actual == null) Fail();

            if (expected.RowIdColumnNumbers.Count() != actual.RowIdColumnNumbers.Count()) Fail("RowIdColumnNumbers count");
            for (var i = 0; i < expected.RowIdColumnNumbers.Count(); i++)
            {
                if (expected.RowIdColumnNumbers.ElementAt(i) !=
                    actual.RowIdColumnNumbers.ElementAt(i))
                    Fail("RowIdColumnNumber " + i);
            }

            if (expected.ComplexProps.Count() != actual.ComplexProps.Count()) Fail("Complex props count");
            for (var i = 0; i < expected.ComplexProps.Count(); i++)
            {
                var x_ = expected.ComplexProps.ElementAt(i);
                var y_ = actual.ComplexProps.ElementAt(i);

                if (x_.name != y_.name)
                    Fail("Complex prop " + i);

                CompareRowIdColumnNumbers(x_.value, y_.value);
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
                .InnerJoin<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .BuildObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(JoinedQueryClass),
                null, 
                new[]
                {
                    ("ThePerson", new ObjectPropertyGraph(
                        typeof(Person),
                        new[]
                        {
                            (0, "##rowid", new int[0], null, null),
                            (4, "Id", new int[0], typeof(long), typeof(long)),
                            (5, "Name", new int[0], typeof(string), typeof(string)),
                            (6, "Gender", new int[0], typeof(Gender), typeof(Gender)),
                            (7, "IsMember", new int[0], typeof(bool), typeof(bool))
                        }, 
                        null, 
                        null)),
                    ("PersonClasses", new ObjectPropertyGraph(
                        typeof(PersonClass),
                        new[]
                        {
                            (1, "##rowid", new int[0], null, null),
                            (2, "PersonId", new int[0], typeof(long), typeof(long)),
                            (3, "ClassId", new int[0], typeof(long), typeof(long))
                        }, 
                        null, 
                        new[]{1}))
                }, 
                new[] { 0 });

                

            Compare(expected, actual);
        }

        [Test]
        public void PropertyGraph_WithRootAndJoins_CreatesCorrectObjectPropertyGraph()
        {
            // arrange
            // act
            var actual = FullyJoinedQuery().BuildObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(JoinedQueryClass),
                null, 
                new[]
                {
                    ("ThePerson", new ObjectPropertyGraph(
                        typeof(Person),
                        new[]
                        {
                            (0, "##rowid", new int[0], null, null),
                            (13, "Id", new int[0], typeof(long), typeof(long)),
                            (14, "Name", new int[0], typeof(string), typeof(string)),
                            (15, "Gender", new int[0], typeof(Gender), typeof(Gender)),
                            (16, "IsMember", new int[0], typeof(bool), typeof(bool))
                        }, 
                        null, 
                        null)),
                    ("PersonClasses", new ObjectPropertyGraph(
                        typeof(PersonClass),
                        new[]
                        {
                            (1, "##rowid", new int[0], null, null),
                            (5, "PersonId", new int[0], typeof(long), typeof(long)),
                            (6, "ClassId", new int[0], typeof(long), typeof(long))
                        }, 
                        null, 
                        new[]{1})),
                    ("Classes", new ObjectPropertyGraph(
                        typeof(Class),
                        new[]
                        {
                            (2, "##rowid", new int[0], null, null),
                            (7, "Id", new int[0], typeof(long), typeof(long)),
                            (8, "Name", new int[0], typeof(string), typeof(string))
                        }, 
                        null, 
                        new[]{2})),
                    ("ClassTags", new ObjectPropertyGraph(
                        typeof(ClassTag),
                        new[]
                        {
                            (3, "##rowid", new int[0], null, null),
                            (9, "ClassId", new int[0], typeof(long), typeof(long)),
                            (10, "TagId", new int[0], typeof(long), typeof(long))
                        }, 
                        null, 
                        new[]{3})),
                    ("Tags", new ObjectPropertyGraph(
                        typeof(Tag),
                        new[]
                        {
                            (4, "##rowid", new int[0], null, null),
                            (11, "Id", new int[0], typeof(long), typeof(long)),
                            (12, "Name", new int[0], typeof(string), typeof(string))
                        }, 
                        null, 
                        new[]{4}))
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
            var actual = FullyJoinedQuery()
                .Map(x => new MappedVersion
                {
                    PersonName = x.ThePerson.Name
                })
                .BuildObjetPropertyGraph<MappedVersion, JoinedQueryClass>();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(MappedVersion),
                new[]
                {
                    (1, "PersonName", new int[0], typeof(string), typeof(string))
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
            var actual = FullyJoinedQuery()
                .Map(x => new MappedVersion
                {
                    PersonName = x.ThePerson.Name,
                    MappedClasses = x.Classes
                        .Select(c => new MappedClass
                        {
                            ClassName = c.Name
                        })
                })
                .BuildObjetPropertyGraph<MappedVersion, JoinedQueryClass>();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(MappedVersion),
                new[]
                {
                    (3, "PersonName", new int[0], typeof(string), typeof(string))
                },
                new[]
                {
                    ("MappedClasses", new ObjectPropertyGraph(
                        typeof(MappedClass),
                        new[]
                        {
                            (4, "ClassName", new int[0], typeof(string), typeof(string))
                        }, 
                        null, 
                        new[]{1,2}))
                }, 
                new[] { 0 });

            Compare(expected, actual);
        }

        [Test]
        public void PropertyGraph_WithMapping3_CreatesCorrectObjectPropertyGraph()
        {
            // arrange
            // act
            var actual = FullyJoinedQuery()
                .Map(x => new MappedVersion
                {
                    PersonName = x.ThePerson.Name,
                    MappedClasses = x.Classes
                        .Select(c => new MappedClass
                        {
                            ClassName = c.Name,
                            TagNames = x.Tags
                                .Select(t => t.Name)

                        })
                })
                .BuildObjetPropertyGraph<MappedVersion, JoinedQueryClass>();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(MappedVersion),
                new[]
                {
                    (5, "PersonName", new int[0], typeof(string), typeof(string))
                },
                new[]
                {
                    ("MappedClasses", new ObjectPropertyGraph(
                        typeof(MappedClass),
                        new[]
                        {
                            (6, "ClassName", new int[0], typeof(string), typeof(string)),
                            (7, "TagNames", new int[]{3, 4}, typeof(IEnumerable<string>), typeof(string))
                        }, 
                        null, 
                        new[]{1,2}))
                }, 
                new[] { 0 });

            Compare(expected, actual);
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
            var actual = FullyJoinedQuery()
                .Map(x => new MappedVersion2
                {
                    PersonName = x.ThePerson.Name,
                    MappedClasses = x.Classes
                        .Select(c => new MappedClass2
                        {
                            ClassName = c.Name,
                            Tags = x.Tags
                                .Select(t => new Tag2 { TagName = t.Name })

                        })
                })
                .BuildObjetPropertyGraph<MappedVersion2, JoinedQueryClass>();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(MappedVersion2),
                new[]
                {
                    (5, "PersonName", new int[0], typeof(string), typeof(string))
                },
                new[]
                {
                    ("MappedClasses", new ObjectPropertyGraph(
                        typeof(MappedClass2),
                        new[]
                        {
                            (6, "ClassName", new int[0], typeof(string), typeof(string))
                        }, 
                        new[]
                        {
                            ("Tags", new ObjectPropertyGraph(
                                typeof(Tag2),
                                new[]
                                {
                                    (7, "TagName", new int[0], typeof(string), typeof(string))
                                }, 
                                null, 
                                new[]{3, 4}))
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
            var actual = FullyJoinedQuery()
                .Map(query => new DifficultScenario
                { 
                    FavouriteClasses = query.Classes
                        .Select(c => new DifficultScenarioInner
                        {
                            TagIds = query.ClassTags
                                .Select(t => t.TagId)
                                .ToArray()
                        })
                        .ToArray()
                })
                .BuildObjetPropertyGraph<DifficultScenario, JoinedQueryClass>();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(DifficultScenario),
                null,
                new[]
                {
                    ("FavouriteClasses", new ObjectPropertyGraph(
                        typeof(DifficultScenarioInner),
                        new[]
                        {
                            (4, "TagIds", new int[]{3}, typeof(long[]), typeof(long))
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
            var actual = FullyJoinedQuery()
                .Map(query => new DifficultScenario2
                { 
                    TagIds = query.ClassTags
                        .Select(t => t.TagId)
                        .ToArray()
                })
                .BuildObjetPropertyGraph<DifficultScenario2, JoinedQueryClass>();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(DifficultScenario2),
                new []
                {
                    (4, "TagIds", new int[]{1,2,3}, typeof(long[]), typeof(long))
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
            var actual = FullyJoinedQuery()
                .Map(query => new DeepJoinedClass
                { 
                    Inner = new DeepJoinedClass
                    {
                        Inner = new DeepJoinedClass
                        {
                            FavouriteClasses = query.Classes
                                .Select(c => new DeepJoinedClassData
                                {
                                    TagIds = query.ClassTags
                                        .Select(t => t.TagId)
                                        .ToArray()
                                })
                                .ToArray()
                        }
                    }
                })
                .BuildObjetPropertyGraph<DeepJoinedClass, JoinedQueryClass>();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(DeepJoinedClass),
                null,
                new []
                {
                    ("Inner", new ObjectPropertyGraph(
                        typeof(DeepJoinedClass),
                        null,
                        new []
                        {
                            ("Inner", new ObjectPropertyGraph
                            (
                                typeof(DeepJoinedClass),
                                null,
                                new []
                                {
                                    ("FavouriteClasses", new ObjectPropertyGraph
                                    (
                                        typeof(DeepJoinedClassData),
                                        new[] 
                                        {
                                            (4, "TagIds", new int[]{3}, typeof(long[]), typeof(long))
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
                .InnerJoin<PersonClass>(nameof(ClassTag), result => result.PersonClasses)
                    .On((q, c) => c.ClassId == Data.Classes.Tennis.Id)
                .BuildObjetPropertyGraph();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(JoinedQueryClass),
                null, 
                new[]
                {
                    ("ThePerson", new ObjectPropertyGraph(
                        typeof(Person),
                        new[]
                        {
                            (0, "##rowid", new int[0], null, null),
                            (4, "Id", new int[0], typeof(long), typeof(long)),
                            (5, "Name", new int[0], typeof(string), typeof(string)),
                            (6, "Gender", new int[0], typeof(Gender), typeof(Gender)),
                            (7, "IsMember", new int[0], typeof(bool), typeof(bool))
                        }, 
                        null, 
                        null)),
                    ("PersonClasses", new ObjectPropertyGraph(
                        typeof(PersonClass),
                        new[]
                        {
                            (1, "##rowid", new int[0], null, null),
                            (2, "PersonId", new int[0], typeof(long), typeof(long)),
                            (3, "ClassId", new int[0], typeof(long), typeof(long))
                        }, 
                        null, 
                        new[]{1}))
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
                .InnerJoin<PersonClass>(result => result.PersonClasses)
                    .On((q, c) => c.PersonId == q.Person.Id)
                .InnerJoin<PersonsData>(result => result.PersonsData)
                    .On((q, c) => c.PersonId == q.Person.Id)
                .Map(x => new DataCellTypeIsArray1Result
                {
                    TheData = x.PersonsData.Data,
                    TheClassIds = x.PersonClasses
                        .Select(c => c.ClassId)
                        .ToArray()
                })
                .BuildObjetPropertyGraph<DataCellTypeIsArray1Result, DataCellTypeIsArray1>();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(DataCellTypeIsArray1Result),
                new[]
                {
                    (3, "TheData", new [] {2}, typeof(byte[]), typeof(byte[])),
                    (4, "TheClassIds", new [] {1}, typeof(long[]), typeof(long))
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
                .InnerJoin<PersonClass>(result => result.PersonClasses)
                    .On((q, c) => c.PersonId == q.Person.Id)
                .InnerJoin<PersonsData>(result => result.PersonsData)
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
                .BuildObjetPropertyGraph<DataCellTypeIsArray2Result, DataCellTypeIsArray2>();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(DataCellTypeIsArray2Result),
                new[]
                {
                    (3, "Data", new [] {2}, typeof(byte[][]), typeof(byte[])),
                    (4, "ClassIds", new [] {1}, typeof(long[]), typeof(long))
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
                .From()
                .Map(x => new SemiPerson(x, x.Gender))
                .BuildObjetPropertyGraph<SemiPerson, Person>(true);

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(SemiPerson),
                new[]
                {
                    (0, "##rowid", new int[0], (Type)null, (Type)null)
                }, 
                null, 
                new[] { 0 },
                simpleConstructorArgs: new[]
                {
                    (5, 1, new int[0], typeof(Gender), typeof(Gender))
                },
                complexConstructorArgs: new [] 
                {
                    (0, typeof(Person), new ObjectPropertyGraph(
                        typeof(Person),
                        new[]
                        {
                            (1, "Id", new int[0], typeof(long), typeof(long)),
                            (2, "Name", new int[0], typeof(string), typeof(string)),
                            (3, "Gender", new int[0], typeof(Gender), typeof(Gender)),
                            (4, "IsMember", new int[0], typeof(bool), typeof(bool))
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
            var actual = FullyJoinedQuery()
                .Map(q => new Outer
                {
                    classes1 = q.PersonClasses
                        .Select(pc => new Inner
                        {
                            classes2 = q.Classes.ToArray()
                        })
                        .ToArray()
                })
                .BuildObjetPropertyGraph<Outer, JoinedQueryClass>();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(Outer),
                null, 
                new[]
                {
                    ("classes1", new ObjectPropertyGraph(
                        typeof(Inner),
                        null,
                        new[] 
                        {
                            ("classes2", new ObjectPropertyGraph(
                                typeof(Class),
                                new[] 
                                {
                                    (3, "Id", new int[0], typeof(long), typeof(long)),
                                    (4, "Name", new int[0], typeof(string), typeof(string))
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
            var actual = FullyJoinedQuery()
                .Map(q => new Outer(q.PersonClasses
                    .Select(pc => new Inner(q.Classes.ToArray()))
                    .ToArray()))
                .BuildObjetPropertyGraph<Outer, JoinedQueryClass>();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(Outer),
                null,
                null, 
                new[] { 0 },
                complexConstructorArgs: new[]
                {
                    (0, typeof(Inner[]), new ObjectPropertyGraph(
                        typeof(Inner),
                        null,
                        null,
                        new[]{1},
                        complexConstructorArgs: new[] 
                        {
                            (0, typeof(Class[]), new ObjectPropertyGraph(
                                typeof(Class),
                                new[] 
                                {
                                    (3, "Id", new int[0], typeof(long), typeof(long)),
                                    (4, "Name", new int[0], typeof(string), typeof(string))
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
            var actual = FullyJoinedQuery()
                .Map(p => p.PersonClasses.Select(pc => new AnotherDifficultCase { ClassId = pc.ClassId }).ToList())
                .BuildObjetPropertyGraph<List<AnotherDifficultCase>, JoinedQueryClass>();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(List<AnotherDifficultCase>),
                new []
                {
                    (2, "ClassId", new int[]{1}, (Type)null, typeof(long)),   
                },
                null, 
                new[] { 0 });

            Compare(expected, actual);
        }

        [Test]
        public void PropertyGraph_ReturnsMultipleComplexArgs_ReturnsCorrectOPG2()
        {
            // arrange
            // act
            var actual = FullyJoinedQuery()
                .Map(p => p.PersonClasses.ToList())
                .BuildObjetPropertyGraph<List<PersonClass>, JoinedQueryClass>();

            // assert
            var expected = new ObjectPropertyGraph(
                typeof(List<PersonClass>),
                new []
                {
                    (2, "PersonId", new int[]{1}, (Type)null, typeof(long)),   
                    (3, "ClassId", new int[]{1}, (Type)null, typeof(long))   
                },
                null, 
                new[] { 0 });

            Compare(expected, actual);
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
        //             ("AnotherPreMapped", new ObjectPropertyGraph(
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

    public static class RootObjectPropertyGraphTestUtils
    {
        public static RootObjectPropertyGraph BuildObjetPropertyGraph<TResult, TMappedFrom>(this Dsl.ISqlBuilder<object, TResult> builder)
        {
            var compiled = (CompiledQuery<object, TResult>)((QueryMapper<Sqlite.SqliteSyntax, object, TMappedFrom, TResult>)builder)
                .Compile();

            return compiled.PropertyGraph;
        }
        
        public static RootObjectPropertyGraph BuildObjetPropertyGraph<TResult, TMappedFrom>(this Dsl.ISqlBuilder<TResult> builder, bool printQuery = true)
        {
            var mapper = (QueryMapper<TResult>)builder;
            var compiled = (CompiledQuery<TResult>)mapper
                .Compile();

            if (printQuery) Console.WriteLine("NOTE: this string might not represent the full query:\n" + compiled.Sql);

            return compiled.PropertyGraph;
        }
        
        public static RootObjectPropertyGraph BuildObjetPropertyGraph<TResult>(this Dsl.IQuery<TResult> builder)
        {
            var compiled = (CompiledQuery<object, TResult>)((QueryBuilder<Sqlite.SqliteSyntax, object, TResult>)builder)
                .Compile();

            return compiled.PropertyGraph;
        }
        
        public static RootObjectPropertyGraph BuildObjetPropertyGraph<TResult>(this Dsl.IQuery<object, TResult> builder)
        {
            var compiled = (CompiledQuery<object, TResult>)((QueryBuilder<Sqlite.SqliteSyntax, object, TResult>)builder)
                .Compile();

            return compiled.PropertyGraph;
        }
    }
}