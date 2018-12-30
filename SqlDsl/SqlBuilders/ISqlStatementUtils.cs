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
        public static IEnumerable<ISelectColumn> GetRowNumberColumns(this ISqlSelectStatement sqlStatement, string columnAlias)
        {
            // if the piece is a parameter, Table will be null
            var col = sqlStatement.SelectColumns[columnAlias].Table?.RowNumberColumn;
            return col == null 
                ? Enumerable.Empty<ISelectColumn>() 
                : sqlStatement.GetRowNumberColumns(col);
        }
        
        public static IEnumerable<int> GetRowNumberColumnIndexes(this ISqlSelectStatement sqlStatement, string columnAlias)
        {
            return sqlStatement
                .GetRowNumberColumns(columnAlias)
                .Select(c => sqlStatement.SelectColumns.IndexOf(c));
        }

        public static int IndexOfColumnAlias(this ISqlSelectStatement sqlStatement, string columnAlias)
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

        public static IEnumerable<ISelectColumn> GetRowNumberColumns(this ISqlSelectStatement sqlStatement, ISelectColumn rowNumberColumn)
        {
            while (sqlStatement.MappingProperties != null)
                sqlStatement = sqlStatement.MappingProperties.InnerStatement;
                
            return rowNumberColumn.Table.GetRowNumberColumns();
        }
        
        public static IEnumerable<ISelectColumn> GetRowNumberColumns(this IQueryTable table)
        {
            var op = new List<ISelectColumn>();
            while (table != null)
            {
                op.Insert(0, table.RowNumberColumn);
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
