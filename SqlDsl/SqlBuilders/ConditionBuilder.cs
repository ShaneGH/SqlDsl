using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

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
        /// <param name="paramaters">A list of parameters which may be added to</param>
        public static (string setupSql, string sql, IEnumerable<string> queryObjectReferences) BuildCondition(
            this ISqlFragmentBuilder sqlBuilder, 
            ParameterExpression queryRoot, 
            ParameterExpression argsParam, 
            OtherParams otherParams, 
            Expression equality, 
            IList<object> paramaters)
        {
            switch (equality.NodeType)
            {
                case ExpressionType.Convert:
                    return sqlBuilder.BuildCondition(queryRoot, argsParam, otherParams, (equality as UnaryExpression).Operand, paramaters);
                case ExpressionType.Call:
                    return sqlBuilder.BuildCallCondition(queryRoot, argsParam, otherParams, equality as MethodCallExpression, paramaters);
                case ExpressionType.AndAlso:
                    return sqlBuilder.BuildAndCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.OrElse:
                    return sqlBuilder.BuildOrCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.Equal:
                    return sqlBuilder.BuildEqualityCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.NotEqual:
                    return sqlBuilder.BuildNonEqualityCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.LessThan:
                    return sqlBuilder.BuildLessThanCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.LessThanOrEqual:
                    return sqlBuilder.BuildLessThanEqualToCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.GreaterThan:
                    return sqlBuilder.BuildGreaterThanCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.GreaterThanOrEqual:
                    return sqlBuilder.BuildGreaterThanEqualToCondition(queryRoot, argsParam, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.MemberAccess:
                    return sqlBuilder.BuildMemberAccessCondition(queryRoot, argsParam, otherParams, equality as MemberExpression, paramaters);
                case ExpressionType.Constant:
                    return BuildConstantCondition(equality as ConstantExpression, paramaters);
                case ExpressionType.NewArrayInit:
                    return BuildNewArrayCondition(sqlBuilder, queryRoot, argsParam, otherParams, equality as NewArrayExpression, paramaters);
                case ExpressionType.Parameter:
                    return BuildParameterExpression(equality as ParameterExpression, argsParam, paramaters);
                default:
                    throw new NotImplementedException($"Cannot compile expression \"{equality}\" to SQL");
            }
        }

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        /// <param name="combinator">The function to actully build the condition</param>
        static ConditionResult BuildBinaryCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression and, IList<object> paramaters, Func<string, string, (string setupSql, string sql)> combinator) =>
            BuildBinaryCondition(
                sqlBuilder, 
                queryRoot, 
                argsParam, 
                otherParams, 
                sqlBuilder.BuildCondition(queryRoot, argsParam, otherParams, and.Left, paramaters), 
                sqlBuilder.BuildCondition(queryRoot, argsParam, otherParams, and.Right, paramaters), 
                paramaters, 
                combinator);

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        /// <param name="combinator">The function to actully build the condition</param>
        static ConditionResult BuildBinaryCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, ConditionResult lhs, ConditionResult rhs, IList<object> paramaters, Func<string, string, (string setupSql, string sql)> combinator, bool checkForEmpty = false)
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
                new[]{rhs.Item1, rhs.Item1, combo.setupSql}.RemoveNulls().JoinString("\n"),
                combo.sql,
                references);
        }

        /// <summary>
        /// Build an and condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildAndCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression and, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, argsParam, otherParams, and, paramaters, sqlBuilder.BuildAndCondition);

        /// <summary>
        /// Build a condition from a method call expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildCallCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, MethodCallExpression call, IList<object> paramaters)
        {
            var (isIn, lhs, rhs) = ReflectionUtils.IsIn(call);
            if (isIn)
                return BuildInCondition(sqlBuilder, queryRoot, argsParam, otherParams, lhs, rhs, paramaters);
                
            throw new NotImplementedException($"Cannot compile expression \"{argsParam}\" to SQL");
        }

        const string Param = @"\s*@p\d+\s*";

        // @p0, @p1, ...
        static readonly Regex MultiParamRegex = new Regex($"^{Param}(,{Param})*$", RegexOptions.Compiled);

        static ConditionResult BuildInCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, Expression lhs, Expression rhs, IList<object> paramaters)
        {
            var l = BuildCondition(sqlBuilder, queryRoot, argsParam, otherParams, lhs, paramaters);
            var r = BuildCondition(sqlBuilder, queryRoot, argsParam, otherParams, rhs, paramaters);

            // TODO: can I relax this condition
            if (!MultiParamRegex.IsMatch(r.sql))
                throw new InvalidOperationException($"The values in an \"IN (...)\" clause must be a real parameter value. " + 
                $"They cannot come from another table:\n{sqlBuilder.BuildInCondition(l.sql, r.sql).sql}");

            // TODO: this method will require find and replace in strings (inefficient)
            // TODO: only array init supported
            r = rhs.NodeType != ExpressionType.NewArrayInit ?
                (
                    r.setupSql, 
                    // if there is only one parameter, it is an array and will need to be
                    // split into parts when rendering
                    $"{r.sql}{SqlStatementConstants.ParamInFlag}", 
                    r.queryObjectReferences) :
                r;

            return sqlBuilder.BuildBinaryCondition(queryRoot, argsParam, otherParams, l, r, paramaters, sqlBuilder.BuildInCondition);
        }

        /// <summary>
        /// Build an or condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildOrCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression or, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, argsParam, otherParams, or, paramaters, sqlBuilder.BuildOrCondition);

        /// <summary>
        /// Build n = condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildEqualityCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression eq, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, argsParam, otherParams, eq, paramaters, sqlBuilder.BuildEqualityCondition);

        /// <summary>
        /// Build a <> condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildNonEqualityCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression neq, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, argsParam, otherParams, neq, paramaters, sqlBuilder.BuildNonEqualityCondition);

        /// <summary>
        /// Build a < condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildLessThanCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression lt, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, argsParam, otherParams, lt, paramaters, sqlBuilder.BuildLessThanCondition);

        /// <summary>
        /// Build a <= condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildLessThanEqualToCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression lte, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, argsParam, otherParams, lte, paramaters, sqlBuilder.BuildLessThanEqualToCondition);

        /// <summary>
        /// Build a > condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildGreaterThanCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression gt, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, argsParam, otherParams, gt, paramaters, sqlBuilder.BuildGreaterThanCondition);

        /// <summary>
        /// Build a >= condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="argsParam">The parameter in the expression which represents the args of the query</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildGreaterThanEqualToCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, ParameterExpression argsParam, OtherParams otherParams, BinaryExpression gte, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, argsParam, otherParams, gte, paramaters, sqlBuilder.BuildGreaterThanEqualToCondition);

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
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildMemberAccessCondition(
            this ISqlFragmentBuilder sqlBuilder,
            ParameterExpression queryRoot, 
            ParameterExpression argsParam,
            OtherParams otherParams, 
            MemberExpression member, 
            IList<object> paramaters)
        {
            // if expression has a definite value, add it to parameters
            var staticValue = ReflectionUtils.GetExpressionStaticObjectValue(member);
            if (staticValue.memberHasStaticValue)
                return AddToParamaters(staticValue.memberStaticValue, paramaters);

            // get name for expression
            var memberAccess = GetMemberQueryObjectName(member);
            if (!memberAccess.memberIsFromQueryObject)
                throw new InvalidOperationException($"Cannot find table for expression ${member}");

            if (memberAccess.rootParam == argsParam)
                return AddArgToParameters(argsParam, member, paramaters);

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
                        .JoinString("."):
                    // TODO: test this case
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
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildConstantCondition(ConstantExpression constant, IList<object> paramaters) => 
            AddToParamaters(constant.Value, paramaters);

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildNewArrayCondition(this ISqlFragmentBuilder sqlBuilder, 
            ParameterExpression queryRoot, 
            ParameterExpression argsParam, 
            OtherParams otherParams, 
            NewArrayExpression array, 
            IList<object> paramaters)
        {
            return array.Expressions
                .Select(e => BuildCondition(
                    sqlBuilder,
                    queryRoot,
                    argsParam,
                    otherParams,
                    e,
                    paramaters))
                .Aggregate(("", "", Enumerable.Empty<string>()), Combine);

            ConditionResult Combine(ConditionResult x, ConditionResult y)
            {

                return BuildBinaryCondition(sqlBuilder, 
                    queryRoot, 
                    argsParam, 
                    otherParams, 
                    x, y, 
                    paramaters,
                    sqlBuilder.BuildCommaCondition,
                    checkForEmpty: true);
            }
        }

        /// <summary>
        /// Build a condition from a parameter expression
        static ConditionResult BuildParameterExpression(ParameterExpression expression, ParameterExpression argsParam, IList<object> paramaters)
        {
            if (expression != argsParam)
                throw new NotImplementedException($"Cannot compile expression \"{expression}\" to SQL");

            return AddToParamaters(QueryArgAccessor.Create(expression), paramaters);
        }

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        /// <param name="value">The parameter value</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult AddToParamaters(object value, IList<object> paramaters)
        {
            paramaters.Add(value == null ? DBNull.Value : value);
            return (null, $"@p{paramaters.Count - 1}", EmptyStrings);
        }

        /// <summary>
        /// Add a query arg getter a parameter
        /// </summary>
        static ConditionResult AddArgToParameters(
            ParameterExpression argsParam,
            Expression propertyAccessor,
            IList<object> paramaters)
        {
            return AddToParamaters(QueryArgAccessor.Create(argsParam, propertyAccessor), paramaters);
        }
    }
}
