using SqlDsl.Query;
using SqlDsl.Utils;
using SqlDsl.Utils.EqualityComparers;
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
            if (col == null)
                return Enumerable.Empty<ISelectColumn>();

            var path = col.IsRowNumberForTable
                .GetPrimaryKeyColumns(context)
                .ToArray();

            if (path.Contains(context.RowNumberColumn))
                return path;
                
            // in this case a column is being used in the
            // context of one of its child properties
            return context.GetPrimaryKeyColumns(col.IsRowNumberForTable);
        }

        /// <summary>
        /// Go through a list of select columns and insert any row ids which other row ids depend on
        /// in the correct places
        /// </summary>
        public static IEnumerable<ISelectColumn> FillOutRIDSelectColumns(this IEnumerable<ISelectColumn> cols)
        {
            return Execute(cols).Distinct();

            IEnumerable<ISelectColumn> Execute(IEnumerable<ISelectColumn> columns)
            {
                columns = columns.Enumerate();
                if (!columns.Any())
                    return Enumerable.Empty<ISelectColumn>();

                var head = columns.First();
                if (head.IsRowNumberForTable == null)
                    return Execute(columns.Skip(1)).Prepend(head);

                return FillOutJoins(head.IsRowNumberForTable)
                    .Select(x => x.RowNumberColumn)
                    .Concat(Execute(columns.Skip(1)));
            }

            IEnumerable<IQueryTable> FillOutJoins(IQueryTable t)
            {
                return t.JoinedFrom
                    .Select(FillOutJoins)
                    .SelectMany()
                    .Append(t);
            }
        }
        
        public static IEnumerable<int> GetRowNumberColumnIndexes(this ISqlSelectStatement sqlStatement, string columnAlias, IQueryTable context)
        {
            var result = sqlStatement
                .GetRowNumberColumns(columnAlias, context)
                .Select(c => sqlStatement.SelectColumns.IndexOf(c));

            return RemoveTrailingMinusOnes(result, columnAlias);
        }

        // /// <summary>
        // /// Get the precedence of 2 tables.
        // /// </summary>
        // /// <returns>
        // /// 0 if they are the same table, -1 if table1 comes before table2, 1 if table2 comes before table1.
        // /// Throws an exception if tables are not related
        // /// </returns>
        // public static int Compare(this ISqlSelectStatement sqlStatement, string tableAlias1, string tableAlias2)
        // {
        //     if (tableAlias1 == tableAlias2)
        //         return 0;

        //     var sqs = (SqlStatementParts.SqlStatement)sqlStatement;
        //     var t1 = sqs.Tables[tableAlias1];
        //     var t2 = sqs.Tables[tableAlias2];

        //     if (t1.GetTableChain(t2, GetPathErrorHandling.ReturnNull, ContextPosition.EnsureContextIsBeforeTable) != null)
        //         return 1;

        //     if (t2.GetTableChain(t1, GetPathErrorHandling.ReturnNull, ContextPosition.EnsureContextIsBeforeTable) != null)
        //         return -1;
                
        //     throw new InvalidOperationException($"Tables {tableAlias1} and {tableAlias2} are unrelated.");
        // }
        
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
