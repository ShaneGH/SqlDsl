using SqlDsl.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDsl.SqlBuilders
{
    public static class ISqlSelectStatementUtils
    {
        /// <summary>
        /// Get a list of pk columns which is a chain from the "forKey" value to the "root", containing the "context"
        /// <summary>
        public static IEnumerable<ICompositeKey> SupplimentPrimaryKeyColumns(this ISqlSelectStatement sqlStatement, ICompositeKey forKey, IQueryTable context)
        {
            // if the piece is a parameter, Table will be null
            if (forKey == null)
                return Enumerable.Empty<ICompositeKey>();

            var path = forKey.Table
                .GetPrimaryKeyColumns(context)
                .ToArray();

            if (path.Contains(context.PrimaryKey))
                return path;
                
            // in this case a column is being used in the
            // context of one of its child properties
            return context.GetPrimaryKeyColumns(forKey.Table);
        }
        
        /// <summary>
        /// Get a list of pk columns for the "columnAlias" containing the "context"
        /// <summary>
        public static IEnumerable<ICompositeKey> GetPrimaryKeyColumns(this ISqlSelectStatement sqlStatement, string columnAlias, IQueryTable context)
        {
            return sqlStatement.SupplimentPrimaryKeyColumns(
                sqlStatement.SelectColumns[columnAlias].PrimaryKey,
                context);
        }

        /// <summary>
        /// Get a list of pk columns for the primary table
        /// <summary>        
        public static IEnumerable<int> GetPrimaryTableKeyColumnIndexes(this ISqlSelectStatement sqlStatement)
        {
            foreach (var k in sqlStatement.PrimaryKey)
            {
                yield return sqlStatement.SelectColumns.IndexOf(k);
            }
        }

        /// <summary>
        /// Go through a list of select columns and insert any row ids which other row ids depend on
        /// in the correct places
        /// </summary>
        public static IEnumerable<ICompositeKey> FillOutRIDSelectColumns(this IEnumerable<ICompositeKey> cols)
        {
            return Execute(cols).Distinct();

            IEnumerable<ICompositeKey> Execute(IEnumerable<ICompositeKey> columns)
            {
                columns = columns.Enumerate();
                if (!columns.Any())
                    return Enumerable.Empty<ICompositeKey>();

                var head = columns.First();
                var tail = columns.Skip(1);

                return FillOutJoins(head.Table)
                    .Select(x => x.PrimaryKey)
                    .Concat(Execute(tail));
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
            return sqlStatement.GetRowNumberColumnIndexes(sqlStatement.GetPrimaryKeyColumns(columnAlias, context));
        }
        
        public static IEnumerable<int> GetRowNumberColumnIndexes(this ISqlSelectStatement sqlStatement, ICompositeKey key, IQueryTable context)
        {
            return sqlStatement.GetRowNumberColumnIndexes(sqlStatement.SupplimentPrimaryKeyColumns(key, context));
        }

        static IEnumerable<int> GetRowNumberColumnIndexes(this ISqlSelectStatement sqlStatement, IEnumerable<ICompositeKey> keys)
        {
            keys = keys.Enumerate();
            return RemoveTrailingMinusOnes(
                keys.SelectMany(GetIndexes), 
                keys
                    .SelectMany(k => k
                        .Select(c => c.Alias))
                    .JoinString(", "));

            IEnumerable<int> GetIndexes(ICompositeKey key)
            {
                foreach (var k in key)
                {
                    yield return sqlStatement.SelectColumns.IndexOf(k);
                }
            }
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
        
        static IEnumerable<int>  RemoveTrailingMinusOnes(IEnumerable<int> result, string columnIdentifierForException)
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
                    throw new InvalidOperationException($"Could not find row id column for column: {columnIdentifierForException}");
            }

            return i == r.Length - 1 ? r : r.Take(i + 1);
        }
        
        public static IEnumerable<IQueryTable> GetTableChain(this IQueryTable table, IQueryTable context, GetPathErrorHandling errorHandling = GetPathErrorHandling.ThrowException)
        {
            return table.GetPath(context, errorHandling);
        }
        
        public static IEnumerable<ICompositeKey> GetPrimaryKeyColumns(this IQueryTable table, IQueryTable context)
        {
            return table.GetTableChain(context).Select(t => t.PrimaryKey);
        }
        
        public static IEnumerable<IQueryTable> GetAllReferencedTables(this IQueryTable table)
        {
            return table.GetTablesInPath();
        }
        
        public static IEnumerable<ICompositeKey> GetAllPrimaryKeyColumns(this IQueryTable table)
        {
            return table.GetAllReferencedTables().Select(t => t.PrimaryKey);
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
