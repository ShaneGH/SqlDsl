using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    public class SqlBuilderException : CompositeException
    {
        readonly MappingPurpose? _mappingPurpose;

        public SqlBuilderException(MappingPurpose mappingPurpose, Expression invalidExpression, Exception innerException = null)
            : this(mappingPurpose, invalidExpression?.ToString(), innerException)
        {
        }

        public SqlBuilderException(MappingPurpose mappingPurpose, string invalidExpressionMessage, Exception innerException = null)
            : this((MappingPurpose?)mappingPurpose, invalidExpressionMessage, innerException)
        {
        }

        public SqlBuilderException(string invalidExpressionMessage, Exception innerException = null)
            : this(null, invalidExpressionMessage, innerException)
        {
        }

        private SqlBuilderException(MappingPurpose? mappingPurpose, string invalidExpressionMessage, Exception innerException = null)
            : base(invalidExpressionMessage, innerException)
        {
            _mappingPurpose = mappingPurpose;
        }
        
        protected override string GetExceptionMessage()
        {
            return (
                _mappingPurpose.HasValue
                    ? $"Error in {_mappingPurpose} expression:" + Environment.NewLine
                    : "")
                + base.GetExceptionMessage();
        }
    }
}