using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.SqlBuilders;
using SqlDsl.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace SqlDsl.Query
{
    public partial class QueryBuilder<TSqlBuilder, TResult>
    {        
        /// <summary>
        /// Get a sql statement and corresponding sql paramaters from the query
        /// </summary>
        public (string sql, IEnumerable<object> paramaters) ToSql() =>  // FROM INTERFACE
            ToSql(filterSelectCols: null);

        /// <summary>
        /// Get a sql statement and corresponding sql paramaters from the query
        /// </summary>
        /// <param name="filterSelectCols">If specified, only add the given columns to the SELECT statement</param>
        public (string sql, IEnumerable<object> paramaters) ToSql(IEnumerable<string> filterSelectCols)
        {
            var result = ToSqlBuilder(filterSelectCols);
            var sql = result.builder.ToSqlString();
            return (result.builder.ToSql(), result.paramaters);
        }

        /// <summary>
        /// Execute the sql query and get a list of results
        /// </summary>
        /// <param name="executor">
        /// An expression to map the selected table to a property on the result
        /// </param>
        public Task<IEnumerable<TResult>> ExecuteAsync(IExecutor executor) =>  // FROM INTERFACE
            Compile().ExecuteAsync(executor);

        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        public ICompiledQuery<TResult> Compile()
        {
            var sqlBuilder = ToSqlBuilder(null);
            return sqlBuilder.builder
                .Compile<TResult>(sqlBuilder.paramaters, QueryParseType.DoNotDuplicate);
        }

        /// <summary>
        /// Create a populated sql builder along with any constants specified in the query
        /// </summary>
        /// <param name="filterSelectCols">If specified, only add the given columns to the SELECT statement</param>
        public (ISqlStatement builder, IEnumerable<object> paramaters) ToSqlBuilder(IEnumerable<string> filterSelectCols)
        {
            if (PrimaryTableMember == null)
                throw new InvalidOperationException("You must set the FROM table before calling ToSql");

            // create output objects
            var param = new List<object>();
            var builder = new SqlStatementBuilder<TSqlBuilder>();

            // Set the SELECT table
            builder.SetPrimaryTable(PrimaryTableName, PrimaryTableMember.Value.name);

            // get all columns from SELECT and JOINs
            var selectColumns = Joins
                .SelectMany((x, i) => ColumnsOf(x.JoinExpression.joinParam.Type)
                    .Select(y => (table: x.JoinedTableProperty.name, column: y)))
                .Concat(ColumnsOf(PrimaryTableMember.Value.type)
                    .Select(y => (table: PrimaryTableMember.Value.name, column: y)));

            // Filter select columns if specified
            if (filterSelectCols != null)
            {
                var cols = new HashSet<string>(filterSelectCols);
                selectColumns = selectColumns.Where(c => c.table == SqlStatementConstants.RootObjectAlias ?
                    cols.Contains(c.column) :
                    cols.Contains($"{c.table}.{c.column}"));
            }

            // Add select columns to builder
            foreach (var col in selectColumns)
            {
                var alias = col.table == SqlStatementConstants.RootObjectAlias ? null : $"{col.table}.{col.column}";
                builder.AddSelectColumn(col.column, col.table, alias);
            }

            // add each join
            foreach (var join in Joins)
            {
                builder.AddJoin(
                    join.JoinType, 
                    join.TableName, 
                    join.JoinExpression.rootObjectParam,
                    join.JoinExpression.joinParam,
                    join.JoinExpression.joinExpression,
                    param,
                    join.JoinedTableProperty.name);
            }

            // add a where clause if specified
            if (WhereClause != null)
                builder.SetWhere(WhereClause.Value.queryRoot, WhereClause.Value.where, param);

            return (builder, param);
        }
    }
}
