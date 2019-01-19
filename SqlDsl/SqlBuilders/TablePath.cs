using System;
using System.Collections.Generic;
using System.Linq;
using SqlDsl.Utils;

namespace SqlDsl.SqlBuilders
{
    public static class TablePath
    {
        public static IEnumerable<IQueryTable> GetPath(this IQueryTable queryTable, IQueryTable mappingContext, GetPathErrorHandling errorHandling = GetPathErrorHandling.ThrowException)
        {
            var result = GetPathRec(queryTable, mappingContext);

            // if no path was found, the context table might be after the
            // query table in the JOIN hirearchy. In this case it is ok
            // to return a path without the context table
            if (result == null)
            {
                var rs = GetPathRec(mappingContext, queryTable)
                    ?.TakeUntilIncludeLast(x => x == queryTable)
                    .ToArray();

                result = rs != null && rs[rs.Length - 1] == queryTable
                    ? rs
                    : null;
            }

            if (result == null && errorHandling == GetPathErrorHandling.ThrowException)  
                throw new InvalidOperationException($"You cannot use {queryTable.Alias} in the context of {mappingContext.Alias}.");

            return result;
        }

        static IEnumerable<IQueryTable> GetPathRec(IQueryTable fromTable, IQueryTable includeTable)
        {
            if (fromTable == includeTable) return PathOfFirsts(fromTable);

            // TODO: is depth first search optimal?
            foreach (var tab in fromTable.JoinedFrom)
            {
                var xs = GetPathRec(tab, includeTable);
                if (xs != null)
                    return xs.Append(fromTable);
            }

            return null;
        }

        static IEnumerable<IQueryTable> PathOfFirsts(IQueryTable forTable)
        {
            if (forTable.JoinedFrom.Length == 0)
                return forTable.ToEnumerable();

            return PathOfFirsts(forTable.JoinedFrom[0]).Append(forTable);
        }

        public static IEnumerable<IQueryTable> GetTablesInPath(this IQueryTable fromTable)
        {
            return fromTable.JoinedFrom
                .SelectMany(jf => GetTablesInPath(jf))
                .Append(fromTable)
                .Distinct();
        }
    }

    public enum GetPathErrorHandling
    {
        ReturnNull,
        ThrowException
    }
}