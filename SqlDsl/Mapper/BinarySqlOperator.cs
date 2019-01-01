using System;
using System.Linq.Expressions;

namespace SqlDsl.Mapper
{
    public enum BinarySqlOperator
    {
        Add,
        Subtract,
        Multiply,
        Divide,
        In,
        Comma,
        Equal,
        NotEqual,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
        And,
        Or
    }

    // todo: move file
    public static class BinarySqlOperatorUtils
    {
        public static BinarySqlOperator ToCombinationType(this ExpressionType e)
        {
            switch (e)
            {
                case ExpressionType.AndAlso:
                    return BinarySqlOperator.And;
                case ExpressionType.OrElse:
                    return BinarySqlOperator.Or;
                case ExpressionType.Add:
                    return BinarySqlOperator.Add;
                case ExpressionType.Subtract:
                    return BinarySqlOperator.Subtract;
                case ExpressionType.Multiply:
                    return BinarySqlOperator.Multiply;
                case ExpressionType.Divide:
                    return BinarySqlOperator.Divide;
                case ExpressionType.Equal:
                    return BinarySqlOperator.Equal;
                case ExpressionType.NotEqual:
                    return BinarySqlOperator.NotEqual;
                case ExpressionType.GreaterThan:
                    return BinarySqlOperator.GreaterThan;
                case ExpressionType.GreaterThanOrEqual:
                    return BinarySqlOperator.GreaterThanOrEqual;
                case ExpressionType.LessThan:
                    return BinarySqlOperator.LessThan;
                case ExpressionType.LessThanOrEqual:
                    return BinarySqlOperator.LessThanOrEqual;
                default:
                    throw new NotSupportedException(e.ToString());
            }
        }
    }
}