using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

// TODO: clean up BuildBinaryCondition/BuildBinaryConditionX
// I think the more complex case (BuildBinaryCondition) is not needed anymore

namespace SqlDsl.SqlBuilders
{   
    using OtherParams = IEnumerable<(ParameterExpression param, string alias)>;

    /// <summary>
    /// string setupSql, string sql, IEnumerable<string> queryObjectReferences
    /// </summary>
    using ConditionResult = ValueTuple<string, string, IEnumerable<string>>;

    /// <summary>
    /// Class to help build SQL conditional statements
    /// </summary>
    public static class ConditionBuilder
    {
        static readonly IEnumerable<string> EmptyStrings = Enumerable.Empty<string>();

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        public static (string setupSql, string sql, IEnumerable<string> queryObjectReferences) BuildCondition(
            this ISqlFragmentBuilder sqlBuilder, 
            ParameterExpression queryRoot, 
            ParameterExpression argsParam, 
            OtherParams otherParams, 
            Expression equality, 
            ParamBuilder parameters)
        {
            switch (equality.NodeType)
            {
                case ExpressionType.Add:
                    return sqlBuilder.BuildAddCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, parameters);
                case ExpressionType.Subtract:
                    return sqlBuilder.BuildSubtractCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, parameters);
                case ExpressionType.Multiply:
                    return sqlBuilder.BuildMultiplyCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, parameters);
                case ExpressionType.Divide:
                    return sqlBuilder.BuildDivideCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, parameters);
                case ExpressionType.OnesComplement:
                    return sqlBuilder.BuildInCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, parameters);
                case ExpressionType.Modulo:
                    return sqlBuilder.BuildCommaCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, parameters);
                case ExpressionType.Convert:
                    return sqlBuilder.BuildCondition(queryRoot, argsParam, otherParams, (equality as UnaryExpression).Operand, parameters);
                case ExpressionType.Call:
                    return sqlBuilder.BuildCallCondition(queryRoot, argsParam, otherParams, equality as MethodCallExpression, parameters);
                case ExpressionType.AndAlso:
                    return sqlBuilder.BuildAndCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, parameters);
                case ExpressionType.OrElse:
                    return sqlBuilder.BuildOrCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, parameters);
                case ExpressionType.Equal:
                    return sqlBuilder.BuildEqualityCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, parameters);
                case ExpressionType.NotEqual:
                    return sqlBuilder.BuildNonEqualityCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, parameters);
                case ExpressionType.LessThan:
                    return sqlBuilder.BuildLessThanCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, parameters);
                case ExpressionType.LessThanOrEqual:
                    return sqlBuilder.BuildLessThanEqualToCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, parameters);
                case ExpressionType.GreaterThan:
                    return sqlBuilder.BuildGreaterThanCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, parameters);
                case ExpressionType.GreaterThanOrEqual:
                    return sqlBuilder.BuildGreaterThanEqualToCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, parameters);
                case ExpressionType.MemberAccess:
                    return sqlBuilder.BuildMemberAccessCondition(queryRoot, argsParam, otherParams, equality as MemberExpression, parameters);
                case ExpressionType.NewArrayBounds:
                    return BuildNewArrayBoundsCondition(sqlBuilder, queryRoot, argsParam, otherParams, equality as NewArrayExpression, parameters);
                case ExpressionType.Constant:
                    return BuildConstantCondition(equality as ConstantExpression, parameters);
                case ExpressionType.Parameter:
                    return BuildParameterCondition(argsParam, equality as ParameterExpression, parameters);
                case ExpressionType.NewArrayInit:
                    return BuildNewArrayCondition(sqlBuilder, queryRoot, argsParam, otherParams, equality as NewArrayExpression, parameters);
                case ExpressionType.ListInit:
                    return BuildNewListCondition(sqlBuilder, queryRoot, argsParam, otherParams, equality as ListInitExpression, parameters);
                default:
                    throw BuildInvalidExpressionException(equality);
            }
        }

        static (string setupSql, string sql, IEnumerable<string> queryObjectReferences) BuildAddCondition(
            this ISqlFragmentBuilder sqlBuilder, 
            ParameterExpression queryRoot, 
            ParameterExpression argsParam, 
            OtherParams otherParams, 
            BinaryExpression equality, 
            ParamBuilder parameters) =>
            BuildBinaryConditionX(sqlBuilder, queryRoot, argsParam, otherParams, equality, parameters, sqlBuilder.BuildAddCondition);

        static (string setupSql, string sql, IEnumerable<string> queryObjectReferences) BuildSubtractCondition(
            this ISqlFragmentBuilder sqlBuilder, 
            ParameterExpression queryRoot, 
            ParameterExpression argsParam, 
            OtherParams otherParams, 
            BinaryExpression equality, 
            ParamBuilder parameters) =>
            BuildBinaryConditionX(sqlBuilder, queryRoot, argsParam, otherParams, equality, parameters, sqlBuilder.BuildSubtractCondition);

        static (string setupSql, string sql, IEnumerable<string> queryObjectReferences) BuildMultiplyCondition(
            this ISqlFragmentBuilder sqlBuilder, 
            ParameterExpression queryRoot, 
            ParameterExpression argsParam, 
            OtherParams otherParams, 
            BinaryExpression equality, 
            ParamBuilder parameters) =>
            BuildBinaryConditionX(sqlBuilder, queryRoot, argsParam, otherParams, equality, parameters, sqlBuilder.BuildMultiplyCondition);

        static (string setupSql, string sql, IEnumerable<string> queryObjectReferences) BuildDivideCondition(
            this ISqlFragmentBuilder sqlBuilder, 
            ParameterExpression queryRoot, 
            ParameterExpression argsParam, 
            OtherParams otherParams, 
            BinaryExpression equality, 
            ParamBuilder parameters) =>
            BuildBinaryConditionX(sqlBuilder, queryRoot, argsParam, otherParams, equality, parameters, sqlBuilder.BuildDivideCondition);

        static (string setupSql, string sql, IEnumerable<string> queryObjectReferences) BuildInCondition(
            this ISqlFragmentBuilder sqlBuilder, 
            ParameterExpression queryRoot, 
            ParameterExpression argsParam, 
            OtherParams otherParams, 
            BinaryExpression equality, 
            ParamBuilder parameters) =>
            BuildBinaryConditionX(sqlBuilder, queryRoot, argsParam, otherParams, equality, parameters, sqlBuilder.BuildInCondition);

        static (string setupSql, string sql, IEnumerable<string> queryObjectReferences) BuildCommaCondition(
            this ISqlFragmentBuilder sqlBuilder, 
            ParameterExpression queryRoot, 
            ParameterExpression argsParam, 
            OtherParams otherParams, 
            BinaryExpression equality, 
            ParamBuilder parameters) =>
            BuildBinaryConditionX(sqlBuilder, queryRoot, argsParam, otherParams, equality, parameters, sqlBuilder.BuildCommaCondition);

        static Exception BuildInvalidExpressionException(Expression expr) => new NotImplementedException($"Cannot compile expression \"{expr}\" to SQL");

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        /// <param name="combinator">The function to actully build the condition</param>
        static ConditionResult BuildBinaryCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression and, ParamBuilder parameters, Func<string, string, (string setupSql, string sql)> combinator) =>
            BuildBinaryCondition(
                sqlBuilder, 
                queryRoot, 
                argsParam, 
                otherParams, 
                sqlBuilder.BuildCondition(queryRoot, argsParam, otherParams, and.Left, parameters), 
                sqlBuilder.BuildCondition(queryRoot, argsParam, otherParams, and.Right, parameters), 
                parameters,
                combinator);

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        /// <param name="combinator">The function to actully build the condition</param>
        static ConditionResult BuildBinaryConditionX(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression and, ParamBuilder parameters, Func<string, string, string> combinator) =>
            BuildBinaryCondition(
                sqlBuilder, 
                queryRoot, 
                argsParam, 
                otherParams, 
                sqlBuilder.BuildCondition(queryRoot, argsParam, otherParams, and.Left, parameters), 
                sqlBuilder.BuildCondition(queryRoot, argsParam, otherParams, and.Right, parameters), 
                parameters,
                (x, y) => (null, combinator(x, y)));

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        /// <param name="combinator">The function to actully build the condition</param>
        static ConditionResult BuildBinaryCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, ConditionResult lhs, ConditionResult rhs, ParamBuilder parameters, Func<string, string, (string setupSql, string sql)> combinator, bool checkForEmpty = false)
        {
            (string setupSql, string sql) combo;
            if (checkForEmpty)
            {
                if (string.IsNullOrWhiteSpace(lhs.Item2))
                    combo = (null, rhs.Item2);
                else if (string.IsNullOrWhiteSpace(rhs.Item2))
                    combo = (null, lhs.Item2);
                else
                    combo = combinator(lhs.Item2, rhs.Item2);
            }
            else
            {
                combo = combinator(lhs.Item2, rhs.Item2);
            }

            var references = lhs.Item3.Concat(rhs.Item3).Enumerate();
            return (
                new[]{rhs.Item1, rhs.Item1, combo.setupSql}.RemoveNullsAndWhitespaces().JoinString(";\n"),
                combo.sql,
                references);
        }
        
        static ConditionResult BuildBinaryConditionX(
            this ISqlFragmentBuilder sqlBuilder, 
            ParameterExpression queryRoot, 
            ParameterExpression argsParam, 
            OtherParams otherParams, 
            ConditionResult lhs, 
            ConditionResult rhs, 
            ParamBuilder parameters, 
            Func<string, string, string> combinator, 
            bool checkForEmpty = false) => BuildBinaryCondition(
                sqlBuilder, 
                queryRoot, 
                argsParam, 
                otherParams, 
                lhs, 
                rhs, 
                parameters, 
                (x, y) => (null, combinator(x, y)), 
                checkForEmpty: checkForEmpty);

        /// <summary>
        /// Build an and condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        static ConditionResult BuildAndCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression and, ParamBuilder parameters) =>
            sqlBuilder.BuildBinaryConditionX(queryRoot, argsParam, otherParams, and, parameters, sqlBuilder.BuildAndCondition);

        /// <summary>
        /// Build a condition from a method call expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        static ConditionResult BuildCallCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, MethodCallExpression call, ParamBuilder parameters)
        {
            var (isIn, lhs, rhs) = ReflectionUtils.IsIn(call);
            if (isIn)
                return BuildInCondition(sqlBuilder, queryRoot, argsParam, otherParams, lhs, rhs, parameters);
                
            throw BuildInvalidExpressionException(argsParam);
        }

        const string Param = @"\s*@p\d+\s*";

        // @p0, @p1, ...
        static readonly Regex MultiParamRegex = new Regex($"^{Param}(,{Param})*$", RegexOptions.Compiled);

        static readonly HashSet<ExpressionType> InPlaceArrayCreation = new HashSet<ExpressionType>
        {
            ExpressionType.NewArrayBounds,
            ExpressionType.NewArrayInit,
            ExpressionType.ListInit
        };

        static ConditionResult BuildInCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, Expression lhs, Expression rhs, ParamBuilder parameters)
        {
            var l = BuildCondition(sqlBuilder, queryRoot, argsParam, otherParams, lhs, parameters);
            var r = BuildCondition(sqlBuilder, queryRoot, argsParam, otherParams, rhs, parameters);
            
            ReflectionUtils.RemoveConvert(rhs);

            // TODO: can I relax this condition
            if (!string.IsNullOrWhiteSpace(r.sql) && !MultiParamRegex.IsMatch(r.sql))
                throw new InvalidOperationException($"The values in an \"IN (...)\" clause must be a real parameter value. " + 
                $"They cannot come from another table:\n{sqlBuilder.BuildInCondition(l.sql, r.sql)}");

            var rhsType = ReflectionUtils.RemoveConvert(rhs).NodeType;
            if (!InPlaceArrayCreation.Contains(rhsType))
            {
                // TODO: this method will require find and replace in strings (inefficient)
                r = (
                    r.setupSql, 
                    // if there is only one parameter, it is an array and will need to be
                    // split into parts when rendering
                    $"{r.sql}{SqlStatementConstants.ParamArrayFlag}", 
                    r.queryObjectReferences);
            }

            return sqlBuilder.BuildBinaryConditionX(queryRoot, argsParam, otherParams, l, r, parameters, sqlBuilder.BuildInCondition);
        }

        /// <summary>
        /// Build an or condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        static ConditionResult BuildOrCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression or, ParamBuilder parameters) =>
            sqlBuilder.BuildBinaryConditionX(queryRoot, argsParam, otherParams, or, parameters, sqlBuilder.BuildOrCondition);

        /// <summary>
        /// Build n = condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        static ConditionResult BuildEqualityCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression eq, ParamBuilder parameters) =>
            sqlBuilder.BuildBinaryConditionX(queryRoot, argsParam, otherParams, eq, parameters, sqlBuilder.BuildEqualityCondition);

        /// <summary>
        /// Build a <> condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        static ConditionResult BuildNonEqualityCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression neq, ParamBuilder parameters) =>
            sqlBuilder.BuildBinaryConditionX(queryRoot, argsParam, otherParams, neq, parameters, sqlBuilder.BuildNonEqualityCondition);

        /// <summary>
        /// Build a < condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        static ConditionResult BuildLessThanCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression lt, ParamBuilder parameters) =>
            sqlBuilder.BuildBinaryConditionX(queryRoot, argsParam, otherParams, lt, parameters, sqlBuilder.BuildLessThanCondition);

        /// <summary>
        /// Build a <= condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        static ConditionResult BuildLessThanEqualToCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression lte, ParamBuilder parameters) =>
            sqlBuilder.BuildBinaryConditionX(queryRoot, argsParam, otherParams, lte, parameters, sqlBuilder.BuildLessThanEqualToCondition);

        /// <summary>
        /// Build a > condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        static ConditionResult BuildGreaterThanCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression gt, ParamBuilder parameters) =>
            sqlBuilder.BuildBinaryConditionX(queryRoot, argsParam, otherParams, gt, parameters, sqlBuilder.BuildGreaterThanCondition);

        /// <summary>
        /// Build a >= condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        static ConditionResult BuildGreaterThanEqualToCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression gte, ParamBuilder parameters) =>
            sqlBuilder.BuildBinaryConditionX(queryRoot, argsParam, otherParams, gte, parameters, sqlBuilder.BuildGreaterThanEqualToCondition);

        /// <summary>
        /// Get the name and root parameter from an expression
        /// </summary>
        static (bool memberIsFromQueryObject, IEnumerable<string> memberQueryObjectParts, ParameterExpression rootParam) GetMemberQueryObjectName(MemberExpression member)
        {
            // expression is "x.Member", return ["Member"] and {x}
            if (member.Expression is ParameterExpression)
                return (true, new [] { member.Member.Name }, member.Expression as ParameterExpression);

            // expression is "x.Member1.Member2", return ["Member1", "Member2"] and {x}
            if (member.Expression is MemberExpression)
                return getMemberQueryObjectName(member.Expression as MemberExpression);

            // expression is "x.Member1.One().Member2", return ["Member1", "Member2"] and {x}
            var oneOf = ReflectionUtils.IsOne(member.Expression) as MemberExpression;
            if (oneOf != null)
                return getMemberQueryObjectName(oneOf);

            return (false, null, null);

            (bool, IEnumerable<string>, ParameterExpression) getMemberQueryObjectName(MemberExpression mem)
            {
                // recurse through child expressions and add result to this member
                var inner = GetMemberQueryObjectName(mem);
                return inner.memberIsFromQueryObject ?
                    (true, inner.memberQueryObjectParts.Concat(new[]{member.Member.Name}), inner.rootParam) :
                    (false, null, null);
            }
        }

        /// <summary>
        /// Build sql for an x.Member expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="member">The member</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        static ConditionResult BuildMemberAccessCondition(
            this ISqlFragmentBuilder sqlBuilder,
            ParameterExpression queryRoot, 
            ParameterExpression argsParam,
            OtherParams otherParams, 
            MemberExpression member, 
            ParamBuilder parameters)
        {
            // if expression has a definite value, add it to parameters
            var staticValue = ReflectionUtils.GetExpressionStaticObjectValue(member);
            if (staticValue.memberHasStaticValue)
                return AddToParamaters(staticValue.memberStaticValue, parameters);

            // get name for expression
            var memberAccess = GetMemberQueryObjectName(member);
            if (!memberAccess.memberIsFromQueryObject)
                throw new InvalidOperationException($"Cannot find table for expression ${member}");

            if (memberAccess.rootParam == argsParam)
                return AddArgToParameters(argsParam, member, parameters);

            var memberName = memberAccess.memberQueryObjectParts.ToList();

            // find the alias for table being referenced
            var paramAlias = otherParams
                .Where(op => op.param == memberAccess.rootParam)
                .Select(op => op.alias)
                .FirstOrDefault();

            string referencedTableAlias = null;
            if (paramAlias != null)
            {
                memberName.Insert(0, paramAlias);
            }
            else
            {
                // param alias is null, the Expression
                // references the query object
                referencedTableAlias = memberName.Count > 1 ? 
                    memberName
                        .Take(memberName.Count - 1)
                        .JoinString(".") :
                    SqlStatementConstants.RootObjectAlias;
            }

            // build prefix for member
            var prefix = memberName.Count > 1 ? 
                sqlBuilder.WrapAlias(memberName
                    .Take(memberName.Count - 1)
                    .JoinString(".")) + "." :
                "";

            return (
                null, 
                $"{prefix}{sqlBuilder.WrapColumn(memberName.Last())}", 
                referencedTableAlias.ToEnumerable()
            );
        }
        
        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        /// <param name="constant">The constant</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        static ConditionResult BuildConstantCondition(ConstantExpression constant, ParamBuilder parameters) => 
            AddToParamaters(constant.Value, parameters);

        static ConditionResult BuildParameterCondition(ParameterExpression argsParam, ParameterExpression parameter, ParamBuilder parameters)
        {
            if (argsParam != parameter)
                throw BuildInvalidExpressionException(parameter);

            return AddToParamaters(QueryArgAccessor.Create(argsParam), parameters);
        }

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        static ConditionResult BuildNewArrayCondition(this ISqlFragmentBuilder sqlBuilder, 
            ParameterExpression queryRoot, 
            ParameterExpression argsParam, 
            OtherParams otherParams, 
            IEnumerable<Expression> elements, 
            ParamBuilder parameters)
        {
            return elements
                .Select(e => BuildCondition(
                    sqlBuilder,
                    queryRoot,
                    argsParam,
                    otherParams,
                    e,
                    parameters))
                .Aggregate(("", "", Enumerable.Empty<string>()), Combine);

            ConditionResult Combine(ConditionResult x, ConditionResult y)
            {

                return BuildBinaryConditionX(sqlBuilder, 
                    queryRoot, 
                    argsParam, 
                    otherParams, 
                    x, y, 
                    parameters,
                    sqlBuilder.BuildCommaCondition,
                    checkForEmpty: true);
            }
        }

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        static ConditionResult BuildNewArrayCondition(this ISqlFragmentBuilder sqlBuilder, 
            ParameterExpression queryRoot, 
            ParameterExpression argsParam, 
            OtherParams otherParams, 
            NewArrayExpression array, 
            ParamBuilder parameters) => BuildNewArrayCondition(sqlBuilder, queryRoot, argsParam, otherParams, array.Expressions, parameters);

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        static ConditionResult BuildNewListCondition(this ISqlFragmentBuilder sqlBuilder, 
            ParameterExpression queryRoot, 
            ParameterExpression argsParam, 
            OtherParams otherParams, 
            ListInitExpression list, 
            ParamBuilder parameters) => BuildNewArrayCondition(
                sqlBuilder, 
                queryRoot, 
                argsParam, 
                otherParams, 
                list.Initializers.Select(GetFirstListAddParam), 
                parameters);

        static Expression GetFirstListAddParam(ElementInit i)
        {
            if (i.Arguments.Count != 1)
                throw new InvalidOperationException("Invalid list initializer.");
            
            return i.Arguments[0];
        }

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        static ConditionResult BuildNewArrayBoundsCondition(this ISqlFragmentBuilder sqlBuilder, 
            ParameterExpression queryRoot, 
            ParameterExpression argsParam, 
            OtherParams otherParams, 
            NewArrayExpression array, 
            ParamBuilder parameters)
        {
            if (array.Expressions.Count != 1)
                throw BuildInvalidExpressionException(array);

            var lengthExpr = array.Expressions[0] as ConstantExpression;                
            if (lengthExpr == null)
                throw BuildInvalidExpressionException(array);

            var type = ReflectionUtils.GetIEnumerableType(array.Type);
            if (type == null)
                throw BuildInvalidExpressionException(array);

            var length = Convert.ToInt32(lengthExpr.Value);
            var defaultV = type.IsValueType ? 
                Expression.Constant(Activator.CreateInstance(type)) : 
                Expression.Constant(type, null);

            array = Expression.NewArrayInit(
                type, 
                new int[length].Select(x => defaultV));

            return BuildCondition(sqlBuilder, queryRoot, argsParam, otherParams, array, parameters);
        }

        /// <summary>
        /// Build a condition from a parameter expression
        /// </summary>
        static ConditionResult BuildParameterExpression(ParameterExpression expression, ParameterExpression argsParam, ParamBuilder parameters)
        {
            if (expression != argsParam)
                throw BuildInvalidExpressionException(expression);

            return AddToParamaters(QueryArgAccessor.Create(expression), parameters);
        }

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        /// <param name="value">The parameter value</param>
        /// <param name="parameters">A list of parameters which may be added to</param>
        static ConditionResult AddToParamaters(object value, ParamBuilder parameters)
        {
            return (null, parameters.AddParam(value == null ? DBNull.Value : value), EmptyStrings);
        }

        /// <summary>
        /// Add a query arg getter a parameter
        /// </summary>
        static ConditionResult AddArgToParameters(
            ParameterExpression argsParam,
            Expression propertyAccessor,
            ParamBuilder parameters)
        {
            return AddToParamaters(QueryArgAccessor.Create(argsParam, propertyAccessor), parameters);
        }
    }
}
