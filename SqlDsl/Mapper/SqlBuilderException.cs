using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    public class SqlBuilderException : Exception
    {
        readonly string _invalidExpressionMessage;
        readonly SqlBuilderException _innerException;
        readonly MappingPurpose _mappingPurpose;

        static char[] NewlineArray = Environment.NewLine.ToArray();

        public override string Message => GetExceptionMessage();

        public SqlBuilderException(MappingPurpose mappingPurpose, Expression invalidExpression, Exception innerException = null)
            : this(mappingPurpose, invalidExpression?.ToString(), innerException)
        {
        }

        public SqlBuilderException(MappingPurpose mappingPurpose, string invalidExpressionMessage, Exception innerException = null)
            : base(null, innerException is SqlBuilderException ? null : innerException)
        {
            _mappingPurpose = mappingPurpose;
            _invalidExpressionMessage = invalidExpressionMessage ?? throw new ArgumentNullException(nameof(invalidExpressionMessage));
            _innerException = innerException as SqlBuilderException;
        }
        
        public string GetExceptionMessage()
        {
            return $"Error in {_mappingPurpose} expression:"
                + Environment.NewLine
                + GetExpressionString()
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
    }
}