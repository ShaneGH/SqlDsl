using System;
using System.Collections.Generic;
using SqlDsl.DataParser;
using SqlDsl.Mapper;
using SqlDsl.Query;
using SqlDsl.UnitTests.FullPathTests.Environment;

namespace SqlDsl.UnitTests
{
    static class TestUtils
    {
        public static Dsl.IQuery<TArg, QueryContainer> FullyJoinedQueryWithArg<TArg>(bool strictJoins = true) => FullyJoinedQueryWithArg<QueryContainer, TArg>(strictJoins );

        public static Dsl.IQuery<TArg, T> FullyJoinedQueryWithArg<T, TArg>(bool strictJoins = true)
            where T : QueryContainer
        {
            return Sql.Query.Sqlite<TArg, T>(strictJoins)
                .From(x => x.ThePerson)
                .InnerJoin(q => q.ThePersonsData)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id);
        }

        public static Dsl.IQuery<TArg, T> FullyLeftJoinedQueryWithArg<T, TArg>(bool strictJoins = true)
            where T : QueryContainer
        {
            return Sql.Query.Sqlite<TArg, T>(strictJoins)
                .From(x => x.ThePerson)
                .LeftJoin(q => q.ThePersonsData)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .LeftJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .LeftJoin<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .LeftJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .LeftJoin<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id);
        }
        
        public static Dsl.IQuery<T> FullyJoinedQuery<T>(bool strictJoins = true)
            where T : QueryContainer
        {
            return Sql.Query.Sqlite<T>(strictJoins)
                .From(x => x.ThePerson)
                .InnerJoin(q => q.ThePersonsData)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .InnerJoin<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .InnerJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .InnerJoin<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id);
        }
        
        public static Dsl.IQuery<T> FullyLeftJoinedQuery<T>(bool strictJoins = true)
            where T : QueryContainer
        {
            return Sql.Query.Sqlite<T>(strictJoins)
                .From(x => x.ThePerson)
                .LeftJoin(q => q.ThePersonsData)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .LeftJoin<PersonClass>(q => q.ThePersonClasses)
                    .On((q, pc) => q.ThePerson.Id == pc.PersonId)
                .LeftJoin<Class>(q => q.TheClasses)
                    .On((q, c) => q.ThePersonClasses.One().ClassId == c.Id)
                .LeftJoin<ClassTag>(q => q.TheClassTags)
                    .On((q, ct) => q.TheClasses.One().Id == ct.ClassId)
                .LeftJoin<Tag>(q => q.TheTags)
                    .On((q, t) => q.TheClassTags.One().TagId == t.Id);
        }
        
        public static Dsl.IQuery<QueryContainer> FullyJoinedQuery(bool strictJoins = true) => FullyJoinedQuery<QueryContainer>(strictJoins);
        
        public static Dsl.IQuery<QueryContainer> FullyLeftJoinedQuery(bool strictJoins = true) => FullyLeftJoinedQuery<QueryContainer>(strictJoins);
        
        public static RootObjectPropertyGraph BuildMappedObjetPropertyGraph<TResult>(this Dsl.IPager<TResult> builder)
        {
            var mapper = (QueryMapper<TResult>)builder;
            var compiled = (CompiledQuery<TResult>)mapper
                .Compile();

            return compiled.PropertyGraph;
        }

        public static RootObjectPropertyGraph BuildObjetPropertyGraph<TResult>(this Dsl.IPager<TResult> builder)
        {
            var select = (SqlSelect<TResult>)builder;
            var compiled = (CompiledQuery<object, TResult>)select
                .Compile();

            return compiled.PropertyGraph;
        }
        
        public static RootObjectPropertyGraph BuildObjetPropertyGraph<TResult>(this Dsl.IQuery<TResult> builder)
        {
            var compiled = (CompiledQuery<object, TResult>)((SqlSelect<object, TResult>)builder)
                .Compile();

            return compiled.PropertyGraph;
        }
    }

    public class QueryContainer
    {
        public Person ThePerson { get; set; }
        public PersonsData ThePersonsData { get; set; }
        public List<PersonClass> ThePersonClasses { get; set; }
        public List<Class> TheClasses { get; set; }
        public List<ClassTag> TheClassTags { get; set; }
        public List<Tag> TheTags { get; set; }
    }
}