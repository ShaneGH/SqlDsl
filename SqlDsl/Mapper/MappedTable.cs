using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;

namespace SqlDsl.Mapper
{
    class MappedTable
    {
        public readonly string From;
        public readonly string To;
        public readonly bool TableresultsAreAggregated;

        public MappedTable(string from, string to, bool tableresultsAreAggregated)
        {
            From = from;
            To = to;
            TableresultsAreAggregated = tableresultsAreAggregated;
        }

        public StrongMappedTable Convert(ISqlStatement statement)
        {
            return new StrongMappedTable(statement.Tables[From], To, TableresultsAreAggregated);
        }
    }
    
    class StrongMappedTable
    {
        public readonly IQueryTable From;
        public readonly string To;
        public readonly bool TableresultsAreAggregated;

        public StrongMappedTable(IQueryTable from, string to, bool tableresultsAreAggregated)
        {
            From = from;
            To = to;
            TableresultsAreAggregated = tableresultsAreAggregated;
        }
    }
}