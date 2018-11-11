using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.SqlBuilders;
using SqlDsl.SqlBuilders.SqlStatementParts;
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
    public partial class QueryBuilder<TSqlBuilder, TArgs, TResult>
    {
        /// <inheritdoc />
        public Task<IEnumerable<TResult>> ToIEnumerableAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).ToIEnumerableAsync(executor, args, logger);

        /// <inheritdoc />
        public IEnumerable<TResult> ToIEnumerable(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).ToIEnumerable(executor, args, logger);
            
        /// <inheritdoc />
        public Task<List<TResult>> ToListAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).ToListAsync(executor, args, logger);

        /// <inheritdoc />
        public List<TResult> ToList(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).ToList(executor, args, logger);
            
        /// <inheritdoc />
        public Task<TResult[]> ToArrayAsync(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).ToArrayAsync(executor, args, logger);

        /// <inheritdoc />
        public TResult[] ToArray(IExecutor executor, TArgs args, ILogger logger = null) =>
            Compile(logger).ToArray(executor, args, logger);

        /// <summary>
        /// Compile the query into something which can be executed multiple times
        /// </summary>
        public ICompiledQuery<TArgs, TResult> Compile(ILogger logger = null)
        {
            var sqlBuilder = ToSqlStatement(null);
            return sqlBuilder.builder
                .Compile<TArgs, TResult>(sqlBuilder.paramaters, QueryParseType.DoNotDuplicate);
        }

        /// <summary>
        /// Create a populated sql builder along with any constants specified in the query
        /// </summary>
        /// <param name="filterSelectCols">If specified, only add the given columns to the SELECT statement</param>
        public (SqlStatementBuilder builder, IEnumerable<object> paramaters) ToSqlStatement(IEnumerable<string> filterSelectCols)
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
                    cols.Contains(c.column.name) :
                    cols.Contains($"{c.table}.{c.column.name}"));
            }

            // Add select columns to builder
            foreach (var col in selectColumns)
            {
                var alias = col.table == SqlStatementConstants.RootObjectAlias ? null : $"{col.table}.{col.column.name}";
                builder.AddSelectColumn(col.column.dataType, col.column.name, col.table, alias);
            }

            // add each join
            foreach (var join in Joins)
            {
                builder.AddJoin(
                    join.JoinType, 
                    join.TableName, 
                    join.JoinExpression.rootObjectParam,
                    join.JoinExpression.queryArgs,
                    join.JoinExpression.joinParam,
                    join.JoinExpression.joinExpression,
                    param,
                    join.JoinedTableProperty.name);
            }

            // add a where clause if specified
            if (WhereClause != null)
                builder.SetWhere(WhereClause.Value.queryRoot, WhereClause.Value.args, WhereClause.Value.where, param);

            return (builder, param);
        }
    }
}
