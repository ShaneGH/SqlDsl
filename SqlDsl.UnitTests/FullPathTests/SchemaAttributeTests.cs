using System.Collections.Generic;
using NUnit.Framework;
using SqlDsl.Schema;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests.FullPathTests
{
    [SqlTestAttribute(SqlType.MySql)]
    public class SchemaAttributeTests : FullPathTestBase
    {
        public SchemaAttributeTests(SqlType testFlavour)
            : base(testFlavour)
        {
        }
        
    
        [Table("Person")]
        public class PersonWithAttributes
        {
            [Column("Id")]
            public long TheId { get; set; }
            
            [Column("Name")]
            public string TheName { get; set; }
        }

    
        [Table("PersonClass")]
        public class PersonClassWithAttributes
        {
            [Column("PersonId")]
            public long ThePersonId { get; set; }

            [Column("ClassId")]
            public long TheClassId { get; set; }
        }

        class JoinedQueryClass
        {
            public PersonWithAttributes ThePerson { get; set; }
            public List<PersonClassWithAttributes> ThePersonClasses { get; set; }
        }

        Dsl.IQuery<JoinedQueryClass> FullyJoinedQuery()
        {
            return Query<JoinedQueryClass>()
                .From<PersonWithAttributes>(x => x.ThePerson)
                .InnerJoin<PersonClassWithAttributes>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.TheId == pc.ThePersonId);
        }


        [Test]
        public void ClassWithTableAndColumnName_GetsCorrectData()
        {
            // arrange
            // act
            var john = Query<PersonWithAttributes>()
                .Where(p => p.TheId == Data.People.John.Id)
                .OrderBy(x => x.TheId)
                .Skip(0)
                .Take(1)
                .First(Executor, logger: Logger);

            // assert
            Assert.AreEqual(Data.People.John.Id, john.TheId);
            Assert.AreEqual(Data.People.John.Name, john.TheName);
        }

        [Test]
        public void ClassWithTableAndColumnName_AndJoins_GetsCorrectData()
        {
            // arrange
            // act
            var john = FullyJoinedQuery()
                .Where(p => p.ThePerson.TheId == Data.People.John.Id)
                .OrderBy(x => x.ThePerson.TheId)
                .Skip(0)
                .Take(1)
                .First(Executor, logger: Logger);

            // assert
            Assert.AreEqual(Data.People.John.Id, john.ThePerson.TheId);
            Assert.AreEqual(Data.People.John.Name, john.ThePerson.TheName);
            Assert.AreEqual(2, john.ThePersonClasses.Count);

            Assert.AreEqual(Data.People.John.Id, john.ThePersonClasses[0].ThePersonId);
            Assert.AreEqual(Data.Classes.Tennis.Id, john.ThePersonClasses[0].TheClassId);
            
            Assert.AreEqual(Data.People.John.Id, john.ThePersonClasses[1].ThePersonId);
            Assert.AreEqual(Data.Classes.Archery.Id, john.ThePersonClasses[1].TheClassId);
        }

        [Test]
        public void ClassWithTableAndColumnName_AndComplexMap_GetsCorrectData()
        {
            // arrange
            // act
            var john = Query<PersonWithAttributes>()
                .Where(p => p.TheId == Data.People.John.Id)
                .OrderBy(x => x.TheId)
                .Map(x => new
                {
                    TheId = x.TheId,
                    TheName = x.TheName
                })
                .Skip(0)
                .Take(1)
                .First(Executor, logger: Logger);

            // assert
            Assert.AreEqual(Data.People.John.Id, john.TheId);
            Assert.AreEqual(Data.People.John.Name, john.TheName);
        }

        [Test]
        public void ClassWithTableAndColumnName_AndSimpleMap_GetsCorrectData()
        {
            // arrange
            // act
            var john = Query<PersonWithAttributes>()
                .Where(p => p.TheId == Data.People.John.Id)
                .OrderBy(x => x.TheId)
                .Map(x => x.TheName)
                .Skip(0)
                .Take(1)
                .First(Executor, logger: Logger);

            // assert
            Assert.AreEqual(Data.People.John.Name, john);
        }

        [Test]
        public void ClassWithTableAndColumnName_AndJoinsWithComplexMap_GetsCorrectData()
        {
            // arrange
            // act
            var john = FullyJoinedQuery()
                .Where(p => p.ThePerson.TheId == Data.People.John.Id)
                .OrderBy(x => x.ThePerson.TheId)
                .Map(x => new
                {
                    ThePerson = x.ThePerson
                })
                .Skip(0)
                .Take(1)
                .First(Executor, logger: Logger);

            // assert
            Assert.AreEqual(Data.People.John.Id, john.ThePerson.TheId);
            Assert.AreEqual(Data.People.John.Name, john.ThePerson.TheName);
        }

        // TODO: all other features, e.g. mapping, map single prop, map whole object
    }
}