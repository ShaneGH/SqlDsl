using System;
using System.Collections.Generic;
using SqlDsl.SqlBuilders;

namespace SqlDsl.Utils.EqualityComparers
{
    /// <summary>
    /// Orders tables beginning with the PrimaryTable and working up the last join
    /// </summary>
    public class TablePrecedenceOrderer : IComparer<IQueryTable>
    {
        public static IComparer<IQueryTable> Instance = new TablePrecedenceOrderer();

        private TablePrecedenceOrderer()
        {
        }

        public int Compare(IQueryTable x, IQueryTable y)
        {
            return CompareRec(x, y) 
                ?? throw new InvalidOperationException($"Cannot find table precendence between {x.Alias} and {y.Alias}");
        }

        int? CompareRec(IQueryTable x, IQueryTable y)
        {
            if (x == null || y == null)
                throw new NotSupportedException();

            if (x == y) return 0;

            if (x.JoinedFrom != null)
            {
                var i = CompareRec(x.JoinedFrom, y);
                if (i != null)
                    return i == 0 ? 1 : i;
            }
            
            if (y.JoinedFrom != null)
            {
                var i = CompareRec(x, y.JoinedFrom);
                if (i != null)
                    return i == 0 ? -1 : i;
            }

            return null;
        }
    }
}