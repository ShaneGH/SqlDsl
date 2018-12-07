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
    }
}