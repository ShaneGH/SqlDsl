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

        public MappedTable(string from, string to)
        {
            From = from;
            To = to;
        }

        public StrongMappedTable Convert(ISqlStatement statement)
        {
            return new StrongMappedTable(statement.Tables[From], To);
        }
    }
    
    class StrongMappedTable
    {
        public readonly IQueryTable From;
        public readonly string To;

        public StrongMappedTable(IQueryTable from, string to)
        {
            From = from;
            To = to;
        }
    }
}