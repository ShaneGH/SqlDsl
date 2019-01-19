using SqlDsl.Query;
using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    public static class ISqlSelectStatementUtils
    {
        public static IEnumerable<ISelectColumn> GetRowNumberColumns(this ISqlSelectStatement sqlStatement, string columnAlias, IQueryTable context)
        {
            var col = sqlStatement.SelectColumns[columnAlias].RowNumberColumn;

            // if the piece is a parameter, Table will be null
            return col == null 
                ? Enumerable.Empty<ISelectColumn>() 
                : col.IsRowNumberForTable.GetPrimaryKeyColumns(context);
        }
        
        public static IEnumerable<int> GetRowNumberColumnIndexes(this ISqlSelectStatement sqlStatement, string columnAlias, IQueryTable context)
        {
            var result = sqlStatement
                .GetRowNumberColumns(columnAlias, context)
                .Select(c => sqlStatement.SelectColumns.IndexOf(c));

            return RemoveTrailingMinusOnes(result, columnAlias);
        }
        
        static IEnumerable<int>  RemoveTrailingMinusOnes(IEnumerable<int> result, string columnAlias)
        {
            int i;
            var r = result.ToArray();
            for (i = r.Length - 1; i >= 0; i--)
            {
                if (r[i] != -1)
                    break;
            }

            for (var j = i - 1; j >= 0; j--)
            {
                if (r[i] == -1)
                    throw new InvalidOperationException($"Could not find row id column for column: {columnAlias}");
            }

            return i == r.Length - 1 ? r : r.Take(i + 1);
        }
        
        public static IEnumerable<IQueryTable> GetTableChain(this IQueryTable table, IQueryTable context, GetPathErrorHandling errorHandling = GetPathErrorHandling.ThrowException)
        {
            return table.GetPath(context, errorHandling);
        }
        
        public static IEnumerable<ISelectColumn> GetPrimaryKeyColumns(this IQueryTable table, IQueryTable context)
        {
            return table.GetTableChain(context).Select(t => t.RowNumberColumn);
        }
        
        public static IEnumerable<IQueryTable> GetAllReferencedTables(this IQueryTable table)
        {
            return table.GetTablesInPath();
        }
        
        public static IEnumerable<ISelectColumn> GetAllPrimaryKeyColumns(this IQueryTable table)
        {
            return table.GetAllReferencedTables().Select(t => t.RowNumberColumn);
        }
        
        public static bool ContainsTable(this ISqlStatement sqlStatement, string tableAlias)
        {
            foreach (var t in sqlStatement.Tables)
            {
                if (t.Alias == tableAlias)
                {
                    return true;
                }
            }

            return false;
        }
    }
}
