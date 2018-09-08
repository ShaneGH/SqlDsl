using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{   
    using OtherParams = IEnumerable<(ParameterExpression param, string alias)>;

    /// <summary>
    /// stirng setupSql, string sql, IEnumerable<string> queryObjectReferences
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
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        public static (string setupSql, string sql, IEnumerable<string> queryObjectReferences) BuildCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, OtherParams otherParams, Expression equality, IList<object> paramaters)
        {
            switch (equality.NodeType)
            {
                case ExpressionType.Convert:
                    return sqlBuilder.BuildCondition(queryRoot, otherParams, (equality as UnaryExpression).Operand, paramaters);
                case ExpressionType.AndAlso:
                    return sqlBuilder.BuildAndCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.OrElse:
                    return sqlBuilder.BuildOrCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.Equal:
                    return sqlBuilder.BuildEqualityCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.NotEqual:
                    return sqlBuilder.BuildNonEqualityCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.LessThan:
                    return sqlBuilder.BuildLessThanCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.LessThanOrEqual:
                    return sqlBuilder.BuildLessThanEqualToCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.GreaterThan:
                    return sqlBuilder.BuildGreaterThanCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.GreaterThanOrEqual:
                    return sqlBuilder.BuildGreaterThanEqualToCondition(queryRoot, otherParams, equality as BinaryExpression, paramaters);
                case ExpressionType.MemberAccess:
                    return sqlBuilder.BuildMemberAccessCondition(queryRoot, otherParams, equality as MemberExpression, paramaters);
                case ExpressionType.Constant:
                    return BuildConstantCondition(equality as ConstantExpression, paramaters);
                default:
                    throw new NotImplementedException($"Cannot compile expression: {equality.NodeType} to SQL");
            }
        }

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        /// <param name="combinator">The function to actully build the condition</param>
        static ConditionResult BuildBinaryCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression and, IList<object> paramaters, Func<string, string, (string setupSql, string sql)> combinator)
        {
            var l = sqlBuilder.BuildCondition(queryRoot, otherParams, and.Left, paramaters);
            var r = sqlBuilder.BuildCondition(queryRoot, otherParams, and.Right, paramaters);
            var combo = combinator(l.Item2, r.Item2);
            var references = l.Item3.Concat(r.Item3).Enumerate();

            return (
                new[]{r.Item1, r.Item1, combo.setupSql}.RemoveNulls().JoinString("\n"),
                combo.sql,
                references);
        }

        /// <summary>
        /// Build an and condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildAndCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression and, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, otherParams, and, paramaters, sqlBuilder.BuildAndCondition);

        /// <summary>
        /// Build an or condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildOrCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression or, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, otherParams, or, paramaters, sqlBuilder.BuildOrCondition);

        /// <summary>
        /// Build n = condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildEqualityCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression eq, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, otherParams, eq, paramaters, sqlBuilder.BuildEqualityCondition);

        /// <summary>
        /// Build a <> condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildNonEqualityCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression neq, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, otherParams, neq, paramaters, sqlBuilder.BuildNonEqualityCondition);

        /// <summary>
        /// Build a < condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildLessThanCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression lt, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, otherParams, lt, paramaters, sqlBuilder.BuildLessThanCondition);

        /// <summary>
        /// Build a <= condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildLessThanEqualToCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression lte, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, otherParams, lte, paramaters, sqlBuilder.BuildLessThanEqualToCondition);

        /// <summary>
        /// Build a > condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildGreaterThanCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression gt, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, otherParams, gt, paramaters, sqlBuilder.BuildGreaterThanCondition);

        /// <summary>
        /// Build a >= condition from an expression
        /// </summary>
        /// <param name="sqlBuilder">The sql builder to use to generate scripts</param>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildGreaterThanEqualToCondition(this ISqlFragmentBuilder sqlBuilder, ParameterExpression queryRoot, OtherParams otherParams, BinaryExpression gte, IList<object> paramaters) =>
            sqlBuilder.BuildBinaryCondition(queryRoot, otherParams, gte, paramaters, sqlBuilder.BuildGreaterThanEqualToCondition);

        /// <summary>
        /// Get a real value from a member expression
        /// </summary>
        /// <returns>memberStaticValue: null if memberHasStaticValue == false
        /// </returns>
        static (bool memberHasStaticValue, object memberStaticValue) GetMemberStaticObjectValue(MemberExpression member)
        {
            var m = member;

            // drill down into the member until we get to the root
            while (member != null)
            {
                // if we get to the root (static property, expression == null)
                if (member.Expression is ConstantExpression || member.Expression == null)
                {
                    // compile the expression
                    var valueGetter = Expression
                        .Lambda<Func<object>>(
                            Expression.Convert(
                                m, typeof(object)))
                        .Compile();

                    // get the expression value
                    return (true, valueGetter());
                }

                member = member.Expression as MemberExpression;
            }

            return (false, null);
        }

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
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="member">The member</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildMemberAccessCondition(
            this ISqlFragmentBuilder sqlBuilder,
            ParameterExpression queryRoot, 
            OtherParams otherParams, 
            MemberExpression member, 
            IList<object> paramaters)
        {
            // if expression has a definite value, add it to parameters
            var staticValue = GetMemberStaticObjectValue(member);
            if (staticValue.memberHasStaticValue)
                return AddToParamaters(staticValue.memberStaticValue, paramaters);

            // get name for expression
            var memberAccess = GetMemberQueryObjectName(member);
            if (!memberAccess.memberIsFromQueryObject)
                throw new InvalidOperationException($"Cannot find table for expression ${member}");

            var memberName = memberAccess.memberQueryObjectParts.ToList();

            // find the alias for table being referenced
            var paramAlias = otherParams
                .Where(op => op.param == memberAccess.rootParam)
                .Select(op => op.alias)
                .FirstOrDefault();

            string referencedTable = null;
            if (paramAlias != null)
            {
                memberName.Insert(0, paramAlias);
            }
            else
            {
                // param alias is null, the Expression
                // references the query object
                referencedTable = memberName.Count > 1 ? 
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
                referencedTable.ToEnumerable()
            );
        }

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult BuildConstantCondition(ConstantExpression constant, IList<object> paramaters) => 
            AddToParamaters(constant.Value, paramaters);

        /// <summary>
        /// Build a condition from an expression
        /// </summary>
        /// <param name="queryRoot">The parameter in the expression which represents the query object</param>
        /// <param name="otherParams">Any other parameters in the expression</param>
        /// <param name="equality">The body of the condition</param>
        /// <param name="paramaters">A list of parameters which may be added to</param>
        static ConditionResult AddToParamaters(object value, IList<object> paramaters)
        {
            paramaters.Add(value);
            return (null, $"@p{paramaters.Count - 1}", EmptyStrings);
        }
    }
}
