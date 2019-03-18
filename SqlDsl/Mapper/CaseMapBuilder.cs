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
    static class CaseMapBuilder
    {
        static readonly MethodInfo When = ReflectionUtils
            .GetMethod(() => Sql.Case.When(true));
            
        public static bool IsCase(MethodCallExpression caseExpression)
        {
            return IsElse(caseExpression).isElse;
        }

        public static (IEnumerable<StringBasedMappedProperty> property, IEnumerable<MappedTable> tables) BuildMap(
            BuildMapState state, 
            ConditionalExpression caseExpression,
            string toPrefix = null)
        {
            var (cases, @else, tables) = GetCasesX(state, caseExpression);
            return CompileCases(
                state,
                cases,
                @else,
                tables.Concat(tables),
                caseExpression.Type,
                toPrefix);
        }

        static (IEnumerable<(StringBasedMappedProperty when, StringBasedMappedProperty then)> cases, StringBasedMappedProperty @else, IEnumerable<MappedTable> tables) GetCasesX(
            BuildMapState state, 
            Expression expr)
        {
            var caseExpression = expr as ConditionalExpression;
            if (caseExpression == null)
            {
                var (elseP, elseTables) = ComplexMapBuilder.BuildMap(
                    state,
                    ReflectionUtils.RemoveConvert(expr));

                var elseProperties = elseP.ToArray();
                if (elseProperties.Length != 1)
                    throw new SqlBuilderException(state.MappingPurpose, caseExpression);

                return (CodingConstants.Empty.Case, elseProperties[0], elseTables);
            }

            var (ifP, ifTables) = ComplexMapBuilder.BuildMap(
                state,
                ReflectionUtils.RemoveConvert(caseExpression.Test));

            var ifProperties = ifP.ToArray();
            if (ifProperties.Length != 1)
                throw new SqlBuilderException(state.MappingPurpose, caseExpression);

            var (thenP, thenTables) = ComplexMapBuilder.BuildMap(
                state,
                ReflectionUtils.RemoveConvert(caseExpression.IfTrue));

            var thenProperties = thenP.ToArray();
            if (thenProperties.Length != 1)
                throw new SqlBuilderException(state.MappingPurpose, caseExpression);

            var (otherCases, @else, otherTables) = GetCasesX(
                state,
                ReflectionUtils.RemoveConvert(caseExpression.IfFalse));

            return (
                otherCases.Prepend((ifProperties[0], thenProperties[0])),
                @else,
                ifTables.Concat(thenTables).Concat(otherTables)
            );
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

            var (cases, tables2) = GetDslCases(state, caseExpression.Object);
            return CompileCases(
                state,
                cases,
                el[0],
                tables.Concat(tables2),
                caseExpression.Type,
                toPrefix);
        }

        static (IEnumerable<(StringBasedMappedProperty when, StringBasedMappedProperty then)> cases, IEnumerable<MappedTable> tables) GetDslCases(
            BuildMapState state, 
            Expression caseExpression)
        {
            if (caseExpression == null)
                return (CodingConstants.Empty.Case, CodingConstants.Empty.MappedTable);

            var thenObject = caseExpression as MethodCallExpression;
            if (thenObject == null)
                throw new SqlBuilderException(state.MappingPurpose, caseExpression);

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

            var (cases, tables) = GetDslCases(state, whenObject.Object);
            return (
                cases.Append((whenP[0], thenP[0])),
                tables.Concat(whenTables).Concat(thenTables)
            );
        }

        private static (bool isElse, Expression result) IsElse(MethodCallExpression expr)
        {
            if (expr.Method.Name == "Else"
                && expr.Object != null 
                && expr.Object.Type.IsGenericType
                && expr.Object.Type.GetGenericTypeDefinition() == typeof(ICase<>))
            {
                return (true, expr.Arguments[0]);
            }

            return (false, null);
        }

        private static (bool isElse, Expression result) IsWhen(MethodCallExpression expr)
        {
            if (expr.Method == When)
            {
                return (true, expr.Arguments[0]);
            }

            if (expr.Method.Name == "When"
                && expr.Object != null 
                && expr.Object.Type.IsGenericType
                && expr.Object.Type.GetGenericTypeDefinition() == typeof(ICase<>))
            {
                return (true, expr.Arguments[0]);
            }

            return (false, null);
        }

        private static (bool isElse, Expression result) IsThen(MethodCallExpression expr)
        {
            if (expr.Method.Name != "Then"
                || expr.Object == null)
                return (false, null);

            if (expr.Object.Type == typeof(ICase))
            {
                return (true, expr.Arguments[0]);
            }
            
            if (expr.Object.Type.IsGenericType
                && expr.Object.Type.GetGenericTypeDefinition() == typeof(ICaseResult<>))
            {
                return (true, expr.Arguments[0]);
            }

            return (false, null);
        }

        static (IEnumerable<StringBasedMappedProperty> property, IEnumerable<MappedTable> tables) CompileCases(
            BuildMapState state,
            IEnumerable<(StringBasedMappedProperty when, StringBasedMappedProperty then)> cases, 
            StringBasedMappedProperty @else,
            IEnumerable<MappedTable> tables,
            Type resultType,
            string toPrefix = null)
        {
            var prop = new StringBasedMappedProperty(
                new CaseSqlExpression<StringBasedElement>(
                    cases.Select(c => (c.when.FromParams, c.then.FromParams)), 
                    @else.FromParams), 
                toPrefix, 
                resultType, 
                state.MappingContext.propertyName, 
                cases
                    .SelectMany(x => new [] { x.when, x.then })
                    .Append(@else)
                    .SelectMany(c => c.PropertySegmentConstructors)
                    .ToArray());

            return (
                prop.ToEnumerable(), 
                tables);
        }
    }
}