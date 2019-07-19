using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using SqlDsl.Utils;

namespace SqlDsl.Utils
{
    public abstract class CompositeException : Exception
    {
        readonly string[] _messageParts;

        public CompositeException(string invalidExpressionMessage, Exception innerException)
            : base(BuildExceptionMessage(invalidExpressionMessage, innerException), RemoveCompositeExceptions(innerException))
        {
            _messageParts = BuildExceptionMessageParts(invalidExpressionMessage, innerException);
        }

        private static string BuildExceptionMessage(string invalidExpressionMessage, Exception innerException)
        {
            return BuildExceptionMessageParts(invalidExpressionMessage, innerException)
                .Select((p, i) => "> " + BuildWhiteSpace(i * 2) + p)
                .JoinString(Environment.NewLine);
        }

        private static string[] BuildExceptionMessageParts(string invalidExpressionMessage, Exception innerException)
        {
            if (innerException == null)
                return new [] { invalidExpressionMessage };

            if (innerException is CompositeException ex)
                return ex._messageParts.Append(invalidExpressionMessage).ToArray();

            return new [] { innerException.Message, invalidExpressionMessage }; 
        }

        private static Exception RemoveCompositeExceptions(Exception e)
        {
            return e is CompositeException
                ? RemoveCompositeExceptions(e.InnerException)
                : e;
        }

        static string BuildWhiteSpace(int length)
        {
            return new string(IEnumerableUtils.Create(length, _ => ' ').ToArray());
        }
    }
}