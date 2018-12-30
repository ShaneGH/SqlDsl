using System.Linq.Expressions;

namespace SqlDsl.Mapper
{
    struct StringBasedElement
    {
        public readonly ParameterExpression ParamRoot;
        public readonly string Param;
        public readonly string AggregatedToTable;
        public readonly string Function;

        public StringBasedElement(ParameterExpression paramRoot, string param, string aggregatedToTable, string function)
        {
            ParamRoot = paramRoot;
            Param = param;
            AggregatedToTable = aggregatedToTable;
            Function = function;
        }
    }
}