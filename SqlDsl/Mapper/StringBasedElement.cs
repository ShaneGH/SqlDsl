using System.Linq.Expressions;

namespace SqlDsl.Mapper
{
    struct StringBasedElement
    {
        public readonly ParameterExpression ParamRoot;
        public readonly string Param;
        public readonly string AggregatedToTable;

        public StringBasedElement(ParameterExpression paramRoot, string param, string aggregatedToTable)
        {
            ParamRoot = paramRoot;
            Param = param;
            AggregatedToTable = aggregatedToTable;
        }
    }
}