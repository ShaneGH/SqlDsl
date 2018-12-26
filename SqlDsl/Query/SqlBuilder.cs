using SqlDsl.DataParser;
using SqlDsl.Dsl;
using SqlDsl.Mapper;
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
    /// <summary>
    /// Build and execute sql queries
    /// </summary>
    public abstract class SqlBuilder<TArgs, TResult> : ISqlBuilder<TArgs, TResult>
    {
        /// <summary>
        /// The name of the table in the SELECT statement
        /// </summary>
        protected abstract string PrimaryTableName { get; }
        
        /// <summary>
        /// The name of the member on the TResult which the primary table is appended to
        /// </summary>
        public abstract (string name, Type type)? PrimaryTableMember { get; }
        
        /// <summary>
        /// The joins applied to the query
        /// </summary>
        public abstract IEnumerable<Join> Joins { get; }
        
        /// <summary>
        /// The WHERE part of the query
        /// </summary>
        protected abstract (ParameterExpression queryRoot, ParameterExpression args, Expression where)? WhereClause { get; }
        
        protected abstract IEnumerable<(ParameterExpression queryRoot, ParameterExpression args, Expression orderExpression, OrderDirection direction)> Ordering { get; }
        
        public readonly ISqlSyntax SqlFragmentBuilder;

        public SqlBuilder(ISqlSyntax sqlSyntax)
        {
            SqlFragmentBuilder = sqlSyntax;
        }
        
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
            var timer = new Timer(true);
            var sqlBuilder = ToSqlStatement();
            var compiled = sqlBuilder.builder
                .Compile<TArgs, TResult>(sqlBuilder.paramaters, QueryParseType.DoNotDuplicate);

            if (logger.CanLogInfo(LogMessages.CompiledQuery))
                logger.LogInfo($"Query compiled in {timer.SplitString()}", LogMessages.CompiledQuery);

            return compiled;
        }

        static readonly string NullString = null;

        /// <summary>
        /// Create a populated sql builder along with any constants specified in the query
        /// </summary>
        /// <param name="filterSelectCols">If specified, only add the given columns to the SELECT statement</param>
        public (SqlStatementBuilder builder, IEnumerable<object> paramaters) ToSqlStatement()
        {
            if (PrimaryTableMember == null)
                throw new InvalidOperationException("You must set the FROM table before calling ToSql");

            // create output objects
            var param = new ParamBuilder();
            var builder = new SqlStatementBuilder(SqlFragmentBuilder);

            // Set the SELECT table
            builder.SetPrimaryTable(PrimaryTableName, PrimaryTableMember.Value.name);

            // get all columns from SELECT and JOINs
            var selectColumns = Joins
                .SelectMany((x, i) => ColumnsOf(x.JoinExpression.joinParam.Type)
                    .Select(y => (table: x.JoinedTableProperty.name, column: y)))
                .Concat(ColumnsOf(PrimaryTableMember.Value.type)
                    .Select(y => (table: PrimaryTableMember.Value.name, column: y)));

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

            // Add select columns to builder
            foreach (var col in selectColumns)
            {
                var alias = col.table == SqlStatementConstants.RootObjectAlias ? col.column.name : $"{col.table}.{col.column.name}";
                builder.AddSelectColumn(col.column.dataType, SqlFragmentBuilder.BuildSelectColumn(col.table, col.column.name), alias, new [] {(col.table, col.column.name, NullString)});
            }

            // add a where clause if specified
            if (WhereClause != null)
                builder.SetWhere(WhereClause.Value.queryRoot, WhereClause.Value.args, WhereClause.Value.where, param);

            // add order by if specified
            foreach (var (queryRoot, args, orderExpression, direction) in Ordering)
                builder.AddOrderBy(queryRoot, args, orderExpression, direction, param);

            return (builder, param.Parameters);
        }

        /// <summary>
        /// A cache of column names for a given type
        /// </summary>
        static readonly ConcurrentDictionary<Type, IEnumerable<(string name, Type dataType)>> Columns = new ConcurrentDictionary<Type, IEnumerable<(string, Type)>>();
        
        /// <summary>
        /// Get all of the column names for a given type
        /// </summary>
        static IEnumerable<(string name, Type dataType)> ColumnsOf(Type t)
        {
            if (Columns.TryGetValue(t, out IEnumerable<(string, Type)> value))
                return value;

            value = GetColumnNames(t)
                .ToList()
                .AsReadOnly();
                
            return Columns.GetOrAdd(t, value);
        }

        /// <summary>
        /// Return all of the property and field names of a type as column names
        /// </summary>
        static IEnumerable<(string name, Type dataType)> GetColumnNames(Type t)
        {
            foreach (var col in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                yield return (col.Name, col.PropertyType);
                
            foreach (var col in t.GetFields(BindingFlags.Public | BindingFlags.Instance))
                yield return (col.Name, col.FieldType);
        }
    }
}