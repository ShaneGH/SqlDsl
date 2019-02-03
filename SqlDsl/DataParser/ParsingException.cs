using System;
using SqlDsl.Utils;

namespace SqlDsl.DataParser
{
    public class ParsingException : CompositeException
    {
        public ParsingException(string message, Exception innerException = null)
            : base(message, innerException)
        {
        }
    }
}