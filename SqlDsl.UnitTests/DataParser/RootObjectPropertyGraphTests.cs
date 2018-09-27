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

namespace SqlDsl.UnitTests.DataParser
{
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

        static QueryBuilder<Sqlite.SqliteBuilder, JoinedQueryClass> FullyJoinedQuery()
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
                    .On((q, t) => q.ClassTags.One().TagId == t.Id) as QueryBuilder<Sqlite.SqliteBuilder, JoinedQueryClass>;
        }

        void Compare(ObjectPropertyGraph expected, ObjectPropertyGraph actual)
        {
            if (expected == null && actual == null) return;
            if (expected == null || actual == null) Fail();

            CompareRowIdColumnNumbers(expected, actual);

            if (expected.SimpleProps.Count() != actual.SimpleProps.Count()) Fail("Simple props count");
            for (var i = 0; i < expected.SimpleProps.Count(); i++)
            {
                var x_ = expected.SimpleProps.ElementAt(i);
                var y_ = actual.SimpleProps.ElementAt(i);

                if (x_.index != y_.index || 
                    x_.name != y_.name)
                    Fail("Simple prop " + i);

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
            var actual = (Sql.Query.Sqlite<JoinedQueryClass>()
                .From<Person>(x => x.ThePerson)
                .InnerJoin<PersonClass>(q => q.PersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId) as QueryBuilder<Sqlite.SqliteBuilder, JoinedQueryClass>)
                .ToSqlBuilder(null)
                .builder
                .BuildObjetPropertyGraph(typeof(JoinedQueryClass), QueryParseType.DoNotDuplicate);

            // assert
            var expected = new ObjectPropertyGraph(
                null, 
                new[]
                {
                    ("ThePerson", new ObjectPropertyGraph(
                        new[]
                        {
                            (0, "##rowid", new int[0].Skip(0)),
                            (4, "Id", new int[0].Skip(0)),
                            (5, "Name", new int[0].Skip(0))
                        }, 
                        null, 
                        null)),
                    ("PersonClasses", new ObjectPropertyGraph(
                        new[]
                        {
                            (1, "##rowid", new int[0].Skip(0)),
                            (2, "PersonId", new int[0].Skip(0)),
                            (3, "ClassId", new int[0].Skip(0))
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
            var actual = FullyJoinedQuery()
                .ToSqlBuilder(null)
                .builder
                .BuildObjetPropertyGraph(typeof(JoinedQueryClass), QueryParseType.DoNotDuplicate);

            // assert
            var expected = new ObjectPropertyGraph(
                null, 
                new[]
                {
                    ("ThePerson", new ObjectPropertyGraph(
                        new[]
                        {
                            (0, "##rowid", new int[0].Skip(0)),
                            (13, "Id", new int[0].Skip(0)),
                            (14, "Name", new int[0].Skip(0))
                        }, 
                        null, 
                        null)),
                    ("PersonClasses", new ObjectPropertyGraph(
                        new[]
                        {
                            (1, "##rowid", new int[0].Skip(0)),
                            (5, "PersonId", new int[0].Skip(0)),
                            (6, "ClassId", new int[0].Skip(0))
                        }, 
                        null, 
                        new[]{1})),
                    ("Classes", new ObjectPropertyGraph(
                        new[]
                        {
                            (2, "##rowid", new int[0].Skip(0)),
                            (7, "Id", new int[0].Skip(0)),
                            (8, "Name", new int[0].Skip(0))
                        }, 
                        null, 
                        new[]{2})),
                    ("ClassTags", new ObjectPropertyGraph(
                        new[]
                        {
                            (3, "##rowid", new int[0].Skip(0)),
                            (9, "ClassId", new int[0].Skip(0)),
                            (10, "TagId", new int[0].Skip(0))
                        }, 
                        null, 
                        new[]{3})),
                    ("Tags", new ObjectPropertyGraph(
                        new[]
                        {
                            (4, "##rowid", new int[0].Skip(0)),
                            (11, "Id", new int[0].Skip(0)),
                            (12, "Name", new int[0].Skip(0))
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
            var actual = (FullyJoinedQuery()
                .Map(x => new MappedVersion
                {
                    PersonName = x.ThePerson.Name
                }) as QueryMapper<Sqlite.SqliteBuilder, JoinedQueryClass, MappedVersion>)
                .ToSqlBuilder()
                .builder
                .BuildObjetPropertyGraph(typeof(MappedVersion), QueryParseType.ORM);

            // assert
            var expected = new ObjectPropertyGraph(
                new[]
                {
                    (5, "PersonName", new int[0].Skip(0))
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
            var actual = (FullyJoinedQuery()
                .Map(x => new MappedVersion
                {
                    PersonName = x.ThePerson.Name,
                    MappedClasses = x.Classes
                        .Select(c => new MappedClass
                        {
                            ClassName = c.Name
                        })
                }) as QueryMapper<Sqlite.SqliteBuilder, JoinedQueryClass, MappedVersion>)
                .ToSqlBuilder()
                .builder
                .BuildObjetPropertyGraph(typeof(MappedVersion), QueryParseType.ORM);

            // assert
            var expected = new ObjectPropertyGraph(
                new[]
                {
                    (5, "PersonName", new int[0].Skip(0))
                },
                new[]
                {
                    ("MappedClasses", new ObjectPropertyGraph(
                        new[]
                        {
                            (6, "ClassName", new int[0].Skip(0))
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
            var actual = (FullyJoinedQuery()
                .Map(x => new MappedVersion
                {
                    PersonName = x.ThePerson.Name,
                    MappedClasses = x.Classes
                        .Select(c => new MappedClass
                        {
                            ClassName = c.Name,
                            TagNames = c
                                .Joined(x.ClassTags)
                                .Joined(x.Tags)
                                .Select(t => t.Name)

                        })
                }) as QueryMapper<Sqlite.SqliteBuilder, JoinedQueryClass, MappedVersion>)
                .ToSqlBuilder()
                .builder
                .BuildObjetPropertyGraph(typeof(MappedVersion), QueryParseType.ORM);

            // assert
            var expected = new ObjectPropertyGraph(
                new[]
                {
                    (5, "PersonName", new int[0].Skip(0))
                },
                new[]
                {
                    ("MappedClasses", new ObjectPropertyGraph(
                        new[]
                        {
                            (6, "ClassName", new int[0].Skip(0)),
                            (7, "TagNames", new int[]{3, 4}.Skip(0))
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
            var actual = (FullyJoinedQuery()
                .Map(x => new MappedVersion2
                {
                    PersonName = x.ThePerson.Name,
                    MappedClasses = x.Classes
                        .Select(c => new MappedClass2
                        {
                            ClassName = c.Name,
                            Tags = c
                                .Joined(x.ClassTags)
                                .Joined(x.Tags)
                                .Select(t => new Tag2 { TagName = t.Name })

                        })
                }) as QueryMapper<Sqlite.SqliteBuilder, JoinedQueryClass, MappedVersion2>)
                .ToSqlBuilder()
                .builder
                .BuildObjetPropertyGraph(typeof(MappedVersion2), QueryParseType.ORM);

            // assert
            var expected = new ObjectPropertyGraph(
                new[]
                {
                    (5, "PersonName", new int[0].Skip(0))
                },
                new[]
                {
                    ("MappedClasses", new ObjectPropertyGraph(
                        new[]
                        {
                            (6, "ClassName", new int[0].Skip(0))
                        }, 
                        new[]
                        {
                            ("Tags", new ObjectPropertyGraph(
                                new[]
                                {
                                    (7, "TagName", new int[0].Skip(0))
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
            public string name;
            public int[] TagIds;
        }

        [Test]
        public void PropertyGraph_WithMapping_DifficultScenario_CreatesCorrectObjectPropertyGraph()
        {
            // arrange
            // act
            var actual = (FullyJoinedQuery()
                .Map(query => new DifficultScenario
                { 
                    FavouriteClasses = query.Classes
                        .Select(c => new DifficultScenarioInner
                        {
                         //   name = c.Name,
                            TagIds = c
                                .Joined(query.ClassTags)
                                .Select(t => t.TagId)
                                .ToArray()
                        })
                        .ToArray()
                }) as QueryMapper<Sqlite.SqliteBuilder, JoinedQueryClass, DifficultScenario>)
                .ToSqlBuilder()
                .builder
                .BuildObjetPropertyGraph(typeof(DifficultScenario), QueryParseType.ORM);

            // assert
            var expected = new ObjectPropertyGraph(
                null,
                new[]
                {
                    ("FavouriteClasses", new ObjectPropertyGraph(
                        new[]
                        {
                            (5, "TagIds", new int[]{3}.Skip(0))
                        }, 
                        null, 
                        new[]{1,2}))
                }, 
                new[] { 0 });

            Compare(expected, actual);
        }

        class DifficultScenario2
        {
            public int[] TagIds;
        }

        [Test]
        public void PropertyGraph_WithMapping_DifficultScenario_CreatesCorrectObjectPropertyGraph2()
        {
            // arrange
            // act
            var actual = (FullyJoinedQuery()
                .Map(query => new DifficultScenario2
                { 
                    TagIds = query.ClassTags
                        .Select(t => t.TagId)
                        .ToArray()
                }) as QueryMapper<Sqlite.SqliteBuilder, JoinedQueryClass, DifficultScenario2>)
                .ToSqlBuilder()
                .builder
                .BuildObjetPropertyGraph(typeof(DifficultScenario), QueryParseType.ORM);

            // assert
            var expected = new ObjectPropertyGraph(
                new []
                {
                    (5, "TagIds", new int[]{1,2,3}.Skip(0))
                },
                null,
                new[] { 0 });

            Compare(expected, actual);
        }
    }
}