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
        readonly string _invalidExpressionMessage;
        readonly CompositeException _innerException;

        public override string Message => GetExceptionMessage();

        public CompositeException(string invalidExpressionMessage, Exception innerException)
            : base(null, innerException is CompositeException ? null : innerException)
        {
            _invalidExpressionMessage = invalidExpressionMessage ?? throw new ArgumentNullException(nameof(invalidExpressionMessage));
            _innerException = innerException as CompositeException;
        }

        private static readonly Regex NewLine = new Regex(@"(\r)?\n");

        protected virtual string GetExceptionMessage()
        {
            return GetExpressionString()
                .SelectMany(x => NewLine.Split(x))
                .Select((x, i) => "> " + BuildWhiteSpace(i * 2) + x)
                .JoinString(Environment.NewLine);
        }
        
        public IEnumerable<string> GetExpressionString()
        {
            if (_innerException == null)
                return _invalidExpressionMessage.ToEnumerable();

            return _innerException
                .GetExpressionString()
                .Append(_invalidExpressionMessage);
        }

        static string BuildWhiteSpace(int length)
        {
            var chars = new char[length];
            for (var i = 0; i < length; i++)
                chars[i] = ' ';
            
            return new string(chars);
        }
        
        public IEnumerable<string> GetFullExceptionString()
        {
            if (_innerException == null)
                return _invalidExpressionMessage.ToEnumerable();

            return _innerException
                .GetFullExceptionString()
                .Append(_invalidExpressionMessage);
        }
    }
}