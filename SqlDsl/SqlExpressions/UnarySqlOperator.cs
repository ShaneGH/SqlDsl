using System;
using System.Linq.Expressions;

namespace SqlDsl.SqlExpressions
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