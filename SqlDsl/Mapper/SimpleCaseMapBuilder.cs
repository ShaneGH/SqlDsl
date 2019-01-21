using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlDsl.Dsl;
using SqlDsl.SqlExpressions;
using SqlDsl.Utils;
using static SqlDsl.Mapper.ComplexMapBuilder;

namespace SqlDsl.Mapper
{
    static class SimpleCaseMapBuilder
    {
        public static bool IsCase(MethodCallExpression caseExpression)
        {
            return IsElse(caseExpression).isElse;
        }

        public static (IEnumerable<StringBasedMappedProperty> property, IEnumerable<MappedTable> tables) BuildMap(
            BuildMapState state, 
            MethodCallExpression caseExpression, 
            MapType nextMap,
            string toPrefix = null)
        {
            var (isElse, elseResult) = IsElse(caseExpression); 
            if (!isElse)
                throw new InvalidOperationException("The method only handles sql case statements");

            var (@else, tables) = ComplexMapBuilder.BuildMap(state, elseResult);
            var el = @else.ToArray();
            if (el.Length != 1)
                throw new SqlBuilderException(state.MappingPurpose, caseExpression.Arguments[0]);

            var (subject, cases, tables2) = GetCases(state, caseExpression.Object, MapType.Other, toPrefix);

            var prop = new StringBasedMappedProperty(
                new SimpleCaseSqlExpression<StringBasedElement>(
                    subject.FromParams,
                    cases.Select(c => (c.when.FromParams, c.then.FromParams)), 
                    el[0].FromParams), 
                toPrefix, 
                caseExpression.Type, 
                state.MappingContext.propertyName, 
                false, 
                cases
                    .SelectMany(x => new [] { x.when, x.then })
                    .Concat(@else)
                    .SelectMany(c => c.PropertySegmentConstructors)
                    .ToArray());

            return (
                prop.ToEnumerable(), 
                tables.Concat(tables2));
        }

        static (StringBasedMappedProperty subject, IEnumerable<(StringBasedMappedProperty when, StringBasedMappedProperty then)> cases, IEnumerable<MappedTable> tables) GetCases(
            BuildMapState state, 
            Expression caseExpression, 
            MapType nextMap,
            string toPrefix = null)
        {
            var thenObject = caseExpression as MethodCallExpression;
            if (thenObject == null)
                throw new SqlBuilderException(state.MappingPurpose, caseExpression);

            var (isSubject, subject) = IsSubject(thenObject);
            if (isSubject)
            {
                var (subjectProperties, subjectTables) = ComplexMapBuilder.BuildMap(state, subject);
                var subjectP = subjectProperties.ToArray();
                if (subjectP.Length != 1)
                    throw new SqlBuilderException(state.MappingPurpose, caseExpression);

                return (subjectP[0], CodingConstants.Empty.Case, subjectTables);
            }

            var (isThen, thenResult) = IsThen(thenObject);
            if (!isThen)
                throw new SqlBuilderException(state.MappingPurpose, thenObject);

            var whenObject = thenObject.Object as MethodCallExpression;
            if (whenObject == null)
                throw new SqlBuilderException(state.MappingPurpose, thenObject.Object);

            var (isWhen, whenCondition) = IsWhen(whenObject);
            if (!isWhen)
                throw new SqlBuilderException(state.MappingPurpose, whenObject);

            var (whenProperties, whenTables) = ComplexMapBuilder.BuildMap(state, whenCondition);
            var (thenProperties, thenTables) = ComplexMapBuilder.BuildMap(state, thenResult);
            
            var whenP = whenProperties.ToArray();
            var thenP = thenProperties.ToArray();

            if (whenP.Length != 1 || thenP.Length != 1)
                throw new SqlBuilderException(state.MappingPurpose, caseExpression);

            var (sjt, cases, tables) = GetCases(state, whenObject.Object, MapType.Other, toPrefix);
            return (
                sjt,
                cases.Append((whenP[0], thenP[0])),
                tables.Concat(whenTables).Concat(thenTables)
            );
        }

        private static (bool isElse, Expression result) IsSubject(MethodCallExpression expr)
        {
            if (expr.Method.Name == "Simple"
                && expr.Method.DeclaringType == typeof(Sql.Case)
                && expr.Object == null)
            {
                return (true, expr.Arguments[0]);
            }

            return (false, null);
        }

        private static (bool isElse, Expression result) IsElse(MethodCallExpression expr)
        {
            if (expr.Method.Name == "Else"
                && expr.Object != null 
                && expr.Object.Type.IsGenericType
                && expr.Object.Type.GetGenericTypeDefinition() == typeof(ISimpleCase<,>))
            {
                return (true, expr.Arguments[0]);
            }

            return (false, null);
        }

        private static (bool isElse, Expression result) IsWhen(MethodCallExpression expr)
        {
            if (expr.Method.Name != "When")
                return (false, null);
                
            if (expr.Object == null 
                || !expr.Object.Type.IsGenericType)
                return (false, null);

            var type = expr.Object.Type.GetGenericTypeDefinition();
            return type == typeof(ISimpleCase<>) || type == typeof(ISimpleCase<,>)
                ? (true, expr.Arguments[0])
                : (false, null);
        }

        private static (bool isElse, Expression result) IsThen(MethodCallExpression expr)
        {
            if (expr.Method.Name != "Then")
                return (false, null);
                
            if (expr.Object == null 
                || !expr.Object.Type.IsGenericType)
                return (false, null);

            var type = expr.Object.Type.GetGenericTypeDefinition();
            return type == typeof(ISimpleCaseResult<>) || type == typeof(ISimpleCaseResult<,>)
                ? (true, expr.Arguments[0])
                : (false, null);
        }
    }
}