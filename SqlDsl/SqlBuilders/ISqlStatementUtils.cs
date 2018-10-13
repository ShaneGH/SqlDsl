using SqlDsl.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlDsl.SqlBuilders
{
    public static class ISqlStatementUtils
    {
        public static IEnumerable<int> GetRowNumberColumnIndexes(this ISqlStatement sqlStatement, string columnAlias)
        {
            var col = sqlStatement.SelectColumns[columnAlias].RowNumberColumnIndex;
            return sqlStatement.GetRowNumberColumnIndexes(col);
        }

        public static int IndexOfColumnAlias(this ISqlStatement sqlStatement, string columnAlias)
        {
            var i = 0;
            foreach (var col in sqlStatement.SelectColumns)
            {
                if (col.Alias == columnAlias)
                    return i;

                i++;
            }

            return -1;
        }

        public static IEnumerable<int> GetRowNumberColumnIndexes(this ISqlStatement sqlStatement, int rowNumberColumnIndex)
        {
            while (sqlStatement.MappingProperties != null)
                sqlStatement = sqlStatement.MappingProperties.InnerStatement;
                
            return sqlStatement.Tables[rowNumberColumnIndex].GetRowNumberColumnIndexes();
        }
        
        public static IEnumerable<int> GetRowNumberColumnIndexes(this IQueryTable table)
        {
            var op = new List<int>();
            while (table != null)
            {
                op.Insert(0, table.RowNumberColumnIndex);
                table = table.JoinedFrom;
            }

            return op.Skip(0);
        }
        
        public static bool JoinIsValid(this ISqlStatement sqlStatement, string from, string to)
        {
            return sqlStatement.Tables[to].JoinedFrom?.Alias == from;
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
