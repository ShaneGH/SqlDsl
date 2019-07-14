using System;
using System.Collections.Generic;
using System.Linq;
using SqlDsl.Mapper;
using SqlDsl.SqlBuilders;

namespace SqlDsl.Utils.EqualityComparers
{
    /// <summary>
    /// Orders tables beginning with the PrimaryTable and working up the last join
    /// </summary>
    public class TablePrecedenceOrderer : IComparer<IQueryTable>
    {
        /// <summary>
        /// The context of the ordering. In practice this will be a table in a mapped Select() call
        /// </summary>
        readonly IQueryTable Context;

        public TablePrecedenceOrderer(IQueryTable context)
        {
            Context = context;
        }

        public int Compare(IQueryTable x, IQueryTable y)
        {
            if (x == null || y == null)
                throw new NotSupportedException();

            if (x == y) return 0;

            foreach (var ts in GetPossibilities())
            {
                if (ts == null)
                    continue;

                foreach (var t in ts)
                {
                    if (t == x)
                        return -1;
                    if (t == y)
                        return 1;
                }
            }

            throw new InvalidOperationException($"Cannot find table precendence between {x.Alias} and {y.Alias}");

            IEnumerable<IEnumerable<IQueryTable>> GetPossibilities()
            {
                // context is more likely to be earlier in the chain
                // order tests accordingly
                // TODO ^^ not sure about the above. Need to test
                yield return Context.GetTableChain(x, GetPathErrorHandling.ReturnNull);
                yield return Context.GetTableChain(y, GetPathErrorHandling.ReturnNull);
                yield return x.GetTableChain(y, GetPathErrorHandling.ReturnNull);
                yield return y.GetTableChain(x, GetPathErrorHandling.ReturnNull);
                yield return x.GetTableChain(Context, GetPathErrorHandling.ReturnNull);
                yield return y.GetTableChain(Context, GetPathErrorHandling.ReturnNull);
            }
        }

        internal static IQueryTable GetSingleMappingContext(QueryElementBasedMappedProperty column)
        {
            var rowNumberTables = column.FromParams
                .GetEnumerable()
                .Where(c => !c.IsParameter)
                .Select(c => c.Column.PrimaryKey.Table)
                .Distinct()
                .ToList();

            var mappingContext = column.MappingContext
                .LastOrDefault(x => rowNumberTables.Contains(x)) ??
                column.MappingContext.FirstOrDefault();

            return mappingContext ?? throw new InvalidOperationException($"You must have at least one mapping context: {column.To}.");
        }

        internal static IQueryTable GetSingleMappingContext(ISelectColumn column)
        {
            var mappingContext = column.MappingContext
                .LastOrDefault(x => x != column.PrimaryKey.Table) ??
                column.MappingContext.FirstOrDefault();

            return mappingContext ?? throw new InvalidOperationException($"You must have at least one mapping context: {column.Alias}.");
        }
    }
}