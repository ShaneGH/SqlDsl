using System;
using System.Linq.Expressions;

namespace SqlDsl.Mapper
{
    public enum UnarySqlOperator
    {
        MaxFunction,
        MinFunction,
        CountFunction,
        SumFunction,
        AverageFunction,
    }
}