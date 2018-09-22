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

        static Dsl.IQuery<JoinedQueryClass> FullyJoinedQuery()
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
                    .On((q, t) => q.ClassTags.One().TagId == t.Id);
        }

        void Compare(ObjectPropertyGraph x, ObjectPropertyGraph y)
        {
            if (x == null && y == null) return;
            if (x == null || y == null) Fail();

            if (x.SimpleProps.Count() != y.SimpleProps.Count()) Fail("Simple props count");
            for (var i = 0; i < x.SimpleProps.Count(); i++)
            {
                var x_ = x.SimpleProps.ElementAt(i);
                var y_ = y.SimpleProps.ElementAt(i);

                if (x_.index != y_.index || 
                    x_.name != y_.name || 
                    x_.rowNumberColumnIds != y_.rowNumberColumnIds)
                    Fail("Simple prop " + i);
            }

            if (x.ComplexProps.Count() != y.ComplexProps.Count()) Fail("Complex props count");
            for (var i = 0; i < x.ComplexProps.Count(); i++)
            {
                var x_ = x.ComplexProps.ElementAt(i);
                var y_ = y.ComplexProps.ElementAt(i);

                if (x_.name != y_.name)
                    Fail("Complex prop " + i);

                Compare(x_.value, y_.value);
            }

            void Fail(string message = null) 
            {
                message = message == null ? "" : (" " + message);
                 Assert.Fail($"Objects are not equal:{message}\n{x}\n{y}");
            }
        }
    }
}