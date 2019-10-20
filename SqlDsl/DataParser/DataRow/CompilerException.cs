using System;
using System.Linq;
using SqlDsl.Utils;

namespace SqlDsl.DataParser.DataRow
{
    public class CompilerException : Exception
    {
        public CompilerException(Type[] forTypes, Exception innerException)
            : base(BuildMessage(forTypes), innerException)
        {
        }

        private static string BuildMessage(Type[] forTypes)
        {
            var types = forTypes.Select(t => t.Name).JoinString(", ");
            return $"Error compiling for types: {types}";
        }
    }
}