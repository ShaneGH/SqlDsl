using System;
using System.Linq.Expressions;

namespace SqlDsl.Utils
{
    /// <summary>
    /// Use static ReplaceParameter method to replace a parameter with another expression
    /// </summary>
    class ParameterReplacer : ExpressionVisitor, IDisposable
    {
        ParameterExpression Parameter;
        Expression Replacement;

        private ParameterReplacer()
        {
        }

        void Init(ParameterExpression parameter, Expression replacement)
        {
            Parameter = parameter;
            Replacement = replacement;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node != Parameter)
                return base.VisitParameter(node);

            return Replacement;
        }

        public void Dispose() => Init(null, null);

        [ThreadStatic]
        static readonly ParameterReplacer Instance = new ParameterReplacer();

        /// <summary>
        /// Replace a parameter with another expression
        /// </summary>
        public static Expression ReplaceParameter(Expression expr, ParameterExpression parameter, Expression replacement)
        {
            using (Instance)
            {
                Instance.Init(parameter, replacement);
                return Instance.Visit(expr);
            }
        }
    }
}